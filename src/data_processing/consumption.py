import pandas as pd
import os
import numpy as np
import holidays
import datetime
from collections import OrderedDict
from collections.abc import Iterable
from typing import Tuple

from src import logger
from src.configs.config_loader import load_config
from src.configs.mappings import dict_cts_or_industry_per_industry_sector
from src.data_access.local_reader import (load_preprocessed_ugr_file_if_exists, 
                                          load_raw_ugr_data, 
                                          load_genisis_wz_sector_mapping_file,
                                          load_activity_driver_consumption,
                                          load_gas_industry_self_consuption,
                                          load_gas_industry_self_consuption_cache)
from src.data_access.api_reader import get_manufacturing_energy_consumption
from src.utils.utils import fix_region_id
from src.data_processing.normalization import normalize_region_ids_rows
# This file contains the functions for the consumption data. Will be used in the pipeline "cunsumption".

def get_ugr_data_ranges(year, force_preprocessing=False):
    """
    Get UGR (Underlying Energy Requirements) data for a specific year. 
    
    Args:
        year (int): The year to get the UGR data for. Availible data is defined in the config file:
            load_config("base_config.yaml")['ugr_genisis_year_start'] 
            load_config("base_config.yaml")['ugr_genisis_year_end']
        force_preprocessing (bool): If True, always preprocess the data even if a processed file exists
    
    Returns:
        pandas.DataFrame: The preprocessed UGR data for the specified year
    """
    logger.info(f"src.data_access.local_reader.get_ugr_data: Getting UGR data for year {year}")


    # 1. Validate the Year
    ugr_first_year = load_config("base_config.yaml")['ugr_genisis_year_start'] 
    ugr_last_year = load_config("base_config.yaml")['ugr_genisis_year_end']
    if not ugr_first_year <= year <= ugr_last_year:
        raise ValueError(f"Year {year} is outside the valid range ({ugr_first_year}-{ugr_last_year}). No Genisis UGR data for this year.")


    # 3. Check for Existing Preprocessed File and force_preprocessing
    # if force_preprocessing is True, the file will be preprocessed again
    df = load_preprocessed_ugr_file_if_exists(year, force_preprocessing)
    if df is not None:
        return df

    # Preprocessing the raw data
    logger.info(f"Preprocessing the UGR raw data for year {year}") 
    raw_data = load_raw_ugr_data()
    raw_data = raw_data.drop(columns=[
        "statistics_code", "statistics_label", 
        "time_code", "1_variable_code", 
        "1_variable_label", "1_variable_attribute_code", 
        "1_variable_attribute_label", "2_variable_code", 
        "2_variable_label", "3_variable_code", 
        "3_variable_label", "value_variable_code", 
        "value_variable_label", "2_variable_label", 
        "3_variable_code", "3_variable_label", 
        "value_variable_code", "value_variable_label"
    ])
    

    # 5. Filter Data for the Given Year
    year_data = raw_data[raw_data["time"] == year]
    
    # Check if we have data for the given year
    if year_data.empty:
        raise ValueError(f"No UGR data available for year {year}")
    

    # 6. Remove Rows with Missing industry_sector Codes and energy carrier codes
    # = removing the "Insgesamt" rows with the sums of energy usage per industry_sector/energy carrier
    year_data = year_data.dropna(subset=["2_variable_attribute_code", "3_variable_attribute_code"])
    

    # 7. Map industry_sector (=WZ) Codes Using the Mapping File
    mapping_df = load_genisis_wz_sector_mapping_file()
    # Create mapping dictionary from Genisis_WZ to WZ
    code_mapping = dict(zip(mapping_df["genisis_industry_code"], mapping_df["industry_code_ranges"]))
    # Apply the mapping in the new column "industry_sector"
    year_data["industry_sector"] = year_data["2_variable_attribute_code"].map(code_mapping)
    """ year_data:
    576 rows x 9 columns
    """
    

    # 8. Process Energy Carrier Data
    # Create energy_type column - mapping of the GENEISI energy carrier codes to our energy carrier names
    def determine_energy_type(code):
        if code == "EKT-02":
            return "power[TJ]"
        elif code == "GAS-01":
            return "gas[TJ]"
        elif code in ["OEL-ERD-01", "KFST-DSL-01", "KFST-OTTO-01", "KFST-FLT-01", 
                     "OEL-H-L-01", "PGH221760", "OEL-SONST"]:
            return "petrol[TJ]"
        else:
            return None
    
    year_data["energy_type"] = year_data["3_variable_attribute_code"].apply(determine_energy_type)
    # Filter out rows with unrecognized energy types
    year_data = year_data[year_data["energy_type"].notna()]
    # Replace "-" values (= no value existing) in the "value" column with 0 and convert to int
    year_data["value"] = (
        pd.to_numeric(year_data["value"].replace("-", 0), errors="coerce")
        .fillna(0.0)
    )
    """ year_data:
    432 rows x 10 columns
    """

    
    # 9. Aggregate the Data
    # Group by industry_sector and energy_type, then sum the values = summing up the energy usage per industry_sector and energy carrier
    grouped_data = year_data.groupby(["industry_sector", "energy_type"])["value"].sum().unstack()
    """ grouped_data:
    48 rows x 3 columns
    industry_sector (=index), power, gas, petrol
    """
    
    
    # 10. Convert Energy Units from TJ to MWh
    # Rename columns
    column_mapping = {
        "power[TJ]": "power[MWh]",
        "gas[TJ]": "gas[MWh]",
        "petrol[TJ]": "petrol[MWh]"
    }

    for col in grouped_data.columns:
        grouped_data[col] = (grouped_data[col] * 1000) / 3.6
    grouped_data = grouped_data.rename(columns=column_mapping)
    

    # 11. Rename and Reorder Columns
    # Ensure all energy types exist in the DataFrame
    for energy_type in ["power", "gas", "petrol"]:
        if energy_type not in grouped_data.columns:
            grouped_data[energy_type] = 0
    
    # Reorder columns
    ordered_columns = ["power[MWh]", "gas[MWh]", "petrol[MWh]"]
    result_df = grouped_data[ordered_columns]
    

    # 12. Save the Preprocessed Data
    # Create directory if it doesn't exist
    processed_dir = load_config("base_config.yaml")['preprocessed_dir']
    processed_file = os.path.join(processed_dir, f"ugr_preprocessed_{year}.csv")
    os.makedirs(processed_dir, exist_ok=True)
    # Save the DataFrame
    result_df.to_csv(processed_file)
    
    
    # 13. Return the DataFrame
    """
    result_df:
    48 rows x 3 columns
    industry_sector (=index), power, gas, petrol
    """
    return result_df


def resolve_ugr_industry_sector_ranges_by_employees(ugr_data_ranges: pd.DataFrame, employees_by_industry_sector_and_regional_ids: pd.DataFrame) -> pd.DataFrame:
    """
    Resolve WZ ranges in consumption data to individual WZ codes based on employee distribution.
    consumption_wz = total_consumption_range * (employee_wz / sum_employees_range)
    
    Parameters:
    -----------
    ugr_data_ranges : pd.DataFrame
        DataFrame with WZ ranges as index and consumption values for gas and power
    employees_by_industry_sector_and_regional_ids : pd.DataFrame
        DataFrame with employees per industry_sector and regional code
    year : int
        Year for data
        
    Returns:
    --------
    pd.DataFrame
        DataFrame with individual WZ codes and their consumption values
    """
        # Create an empty DataFrame to store the result
    consumption_by_wz = pd.DataFrame(columns=ugr_data_ranges.columns)
    
    # Convert employees dataframe to national totals
    employees_by_WZ = employees_by_industry_sector_and_regional_ids.sum(axis=1)
    
    # Process each entry in the consumption data
    for wz_code, row in ugr_data_ranges.iterrows():
        # Check if the index is a range (contains a hyphen)
        if isinstance(wz_code, str) and '-' in wz_code:
            # Extract the range bounds
            start_wz, end_wz = map(int, wz_code.split('-'))
            wz_range = range(start_wz, end_wz + 1)
            
            # Get total employees in this range
            total_employees_in_range = sum(employees_by_WZ.get(wz, 0) for wz in wz_range)
            
            if total_employees_in_range > 0:
                # Distribute consumption based on employee ratio
                for wz in wz_range:
                    if wz in employees_by_WZ:
                        employee_ratio = employees_by_WZ[wz] / total_employees_in_range
                        
                        # Allocate consumption according to employee ratio
                        consumption_values = {
                            col: row[col] * employee_ratio for col in row.index
                        }
                        consumption_by_wz.loc[wz] = consumption_values
            else:
                # If no employees in range, distribute equally
                for wz in wz_range:
                    consumption_values = {
                        col: row[col] / len(wz_range) for col in row.index
                    }
                    consumption_by_wz.loc[wz] = consumption_values
        else:
            # If not a range, keep as is
            consumption_by_wz.loc[wz_code] = row
    
    # Ensure WZ codes are integers
    if all(isinstance(idx, (int, np.integer)) or (isinstance(idx, str) and idx.isdigit()) 
           for idx in consumption_by_wz.index):
        consumption_by_wz.index = consumption_by_wz.index.astype(int)


    #validate result: consumption sum of the rows must be close to equal before and after (due to rounding)
    # sum of all cells ugr_data_ranges must be equal to sum of all cells consumption_by_wz
    total_ugr = ugr_data_ranges.sum().sum()
    total_wz = consumption_by_wz.sum().sum()
    relative_diff = abs(total_ugr - total_wz) / max(total_ugr, total_wz)
    if relative_diff > 0.00001:
        raise ValueError("Consumption sum mismatch between ugr_data_ranges and consumption_by_wz (difference > 0.001%)")
    
    return consumption_by_wz



def project_consumption(consumption, year_dataset, year_future):
    """
    DISS 4.5
    project energy demand per wz to given year using activity drivers from
    input files. For industry gross value added (gva) per branch is used, for
    CTS energy reference area per branch is used, which is derived from
    projected number of employees

    Projects consumption data from publications to future year using different
    demand driver per sector. for industry use projected gross value added per
    branch and for CTS use projected consumption area per branch. drivers are
    imported from data_in folder.
    """

    # Validate Inputs: activity drivers are only available for 2015-2050
    if year_dataset not in range(2015, 2050) or year_future not in range(2015, 2050):
        raise ValueError("year_dataset must be between 2015 and 2050. Use the historical consumption!")
    
    # 2. Get Activity Drivers = Mengeneffekt
    activity_drivers = load_activity_driver_consumption()

    # 3. group industry sectors
    #TODO: wieso? df_driver_total = group_industry_sectors(activity_drivers)
    df_driver_total = activity_drivers

    # 4. normalize activity drivers year_dataset
    df_driver_norm = df_driver_total.apply(lambda x: x/x.loc[year_dataset])

    # 5. get the wanted year
    df_driver_norm_year = df_driver_norm.loc[year_future]

    # 6. multiply with consumption
    consumption_projected = consumption.mul(df_driver_norm_year, axis=0)

    return consumption_projected
    


def get_total_gas_industry_self_consuption(year, force_preprocessing=False):
    """
    NEW
    Returns the industry self consumption of a year in MWh.
    
    original source (UGR) does not include gas consumption for self generation in industrial sector
    get gas consumption for self_generation from German energy balance

    in the bilanz<year>d.xlsx file it is the cell: Naturgase_Erdgas/Erdölgas in row "Industriewärmekraftwerke (nur für Strom)"

    preprocessing in the file load_config("base_config.yaml")['gas_industry_self_consumption_cache_file']

    Returns
    ------------
    number with the total industrygas self consumption in MWh
    """

       # Validate year and adjust to available data range
    if year < 2000 or year > 2050:
        raise ValueError("`year` must be between 2000 and 2050")
    if year < 2007:
        year = 2007
    elif year >= 2020:
        year = 2019
    # Otherwise, if year is between 2007 and 2019, we use it as is.

    # Load the cache DataFrame.
    # This should be a DataFrame with a "year" column and a "gas_industry_self_consumption" column.
    cache_df = load_gas_industry_self_consuption_cache()  # Assumes a function that returns a DataFrame

    # Load existing cache (expects a DataFrame with columns "year" and "gas_industry_self_consumption")
    cache_df = load_gas_industry_self_consuption_cache()  # This function should return a DataFrame.
    
    # If not forcing preprocessing and the cache already contains an entry for this year, return that value.
    if not force_preprocessing and not cache_df.empty and (year in cache_df['year'].values):
        return cache_df.loc[cache_df['year'] == year, "gas_industry_self_consumption"].iloc[0]
    
    # Load the gas industry self-consumption data for the adjusted year.
    df_balance = load_gas_industry_self_consuption(year)
    
    # Rename columns as needed.
    df_balance.rename(
        columns={
            "Unnamed: 1": "Zeile",
            "Unnamed: 24": "Grubengas",
            "Naturgase": "Erdgas in Mio kWh",
        },
        inplace=True,
    )
    
    # Drop the first three rows (assuming they are header/info rows)
    df_balance.drop([0, 1, 2], inplace=True)
    
    # Set 'Zeile' as the index.
    df_balance.set_index("Zeile", inplace=True)
    
    # Extract the natural gas consumption value from row with index 12.
    # The value is in Mio kWh (i.e. GWh), so multiply by 1000 to convert to MWh.
    GV_slf_gen_global = df_balance["Erdgas in Mio kWh"].loc[12] * 1000

    # If force_preprocessing is True, remove any cached entry for this year.
    if force_preprocessing and not cache_df.empty:
        cache_df = cache_df[cache_df["year"] != year]
    
    # Prepare the new row; ensure that the new row's types match:
    new_row = pd.DataFrame({
        "year": [int(year)],
        "gas_industry_self_consumption": [GV_slf_gen_global]
    })
    
    # Concatenate the new row with the cache.
    updated_cache = pd.concat([cache_df, new_row], ignore_index=True)
    
    # Explicitly convert the "year" column to integers.
    # Using an apply ensures that each value is cast to int.
    updated_cache["year"] = updated_cache["year"].apply(lambda x: int(x) if pd.notnull(x) else x)
    
    # Save the updated cache back to CSV.
    file_path_cache = load_config("base_config.yaml")['gas_industry_self_consumption_cache_file']
    updated_cache.to_csv(file_path_cache, index=False)
    
    return GV_slf_gen_global



def calculate_self_generation(consumption_df: pd.DataFrame, total_gas_self_consuption: float, decomposition_factors: pd.DataFrame, year: int) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:

    """
    Computes power and gas self-generation metrics and appends them to the input consumption DataFrame.
    
    Parameters:
        consumption_df (pd.DataFrame): UGR data with power, gas, and natural gas consumption.
        decomposition_factors (pd.DataFrame): Contains self-generation shares per industry.
    
    Returns:
        Tuple:
            - pd.DataFrame: Enriched DataFrame with additional self-generation columns.
            - pd.Series: Power self-generation factor per industry sector.
            - pd.Series: Gas no-selfgen-to-total ratio per industry sector.
    """
    # Copy to avoid modifying the original input
    df = consumption_df.copy()
    
    # Rename columns for clarity
    df.rename(columns={
        "power[MWh]": "power_incl_selfgen[MWh]",
        "gas[MWh]": "gas_no_selfgen[MWh]"
    }, inplace=True)
    
    # Self-generation factor for power by industry
    selfgen_factor_power = decomposition_factors['Strom Eigenerzeugung']
    df['power_self_generation[MWh]'] = df['power_incl_selfgen[MWh]'] * selfgen_factor_power
    
    # Share of self-generation per industry
    df['factor_selfgen_of_total_power'] = (df['power_self_generation[MWh]'] / df['power_self_generation[MWh]'].sum())
    
    # Allocate gas self-generation based on power distribution
    df['gas_only_selfgen[MWh]'] = df['factor_selfgen_of_total_power'] * total_gas_self_consuption
    df['gas_incl_selfgen[MWh]'] = df['gas_no_selfgen[MWh]'] + df['gas_only_selfgen[MWh]']
    
    # Final ratio: gas w/o selfgen / total gas
    df['factor_gas_no_selfgen'] = df['gas_no_selfgen[MWh]'] / df['gas_incl_selfgen[MWh]']

    # fill the missing values with 1 (happens if there is no gas consumption and the above valculation tries deviding by 0)
    df.fillna(1, inplace=True)

    # cache factor_gas_no_selfgen
    template = load_config("base_config.yaml")['factor_gas_no_selfgen_cache_file']
    path = template.format(year=year)
    df[['factor_gas_no_selfgen']].to_csv(path, index=True, index_label='industry_sector')
    
    # Return both the enriched DataFrame and the key factors
    return df, df['factor_selfgen_of_total_power'], df['factor_gas_no_selfgen']


def get_regional_energy_consumption(year) -> pd.DataFrame:
    """
    Returns the regional energy consumption for a given year from JEVI
    Database: 'spatial', table_id=15

    Returns:
        pd.DataFrame:
            - index: regional_ids [normalized 400 regional_ids]
            - columns: power[MWh], gas[MWh]
    """
    # Check if year is in valid range
    if year not in range(2000, 2051):
        raise ValueError("`year` must be between 2000 and 2050")

    # API spacial_id=15 has data for years 2003-2017
    # If year is outside this range, use closest available year
    if year < 2003:
        year_to_use = 2003
        logger.info('Regional energy consumption of 2003 was used for calibration of industrial energy consumption.')
    elif year > 2017:
        year_to_use = 2017
        logger.info('Regional energy consumption of 2017 was used for calibration of industrial energy consumption.')
    else:
        year_to_use = year
    
    # Get data "Energieverwendung in der Industrie je LK" spacial_id=15
    data = get_manufacturing_energy_consumption(year=year_to_use)


    # Rename 'value' column to 'consumption[GJ]' = thousand MJ
    data = data.rename(columns={"value": "consumption[GJ]"})
    # Convert 'consumption[GJ]' to MWh
    data['consumption[MWh]'] = data['consumption[GJ]'] / 3.6
    
    # transform the id_region to ags_lk standard format
    data["regional_id"] = data["id_region"].apply(fix_region_id)

    # Filter rows to keep only those with "2" or "4" in the "ET" column (ET=energy type)
    # Extract energy type (ET) from internal_id: 2=gas, 4=power
    # Filter to keep only gas (ET=2) and power (ET=4) rows
    data = data[data['internal_id[0]'].isin([2, 4])]
    data = data.rename(columns={"internal_id[0]": "energy_carrier"})
    
    # Create a pivot table to get consumption by energy type
    data_pivot = data.pivot_table(
        index='regional_id', 
        columns='energy_carrier', 
        values='consumption[MWh]',
        aggfunc='sum'
    ).reset_index()
    
    # Rename columns for clarity
    data_pivot.rename(columns={
        2: 'power[MWh]',
        4: 'gas[MWh]'
    }, inplace=True)
    
    # Fill NaN values with 0 for districts that might be missing one type of consumption
    data_pivot.fillna(0, inplace=True)
    
    # Use the pivoted data
    data = data_pivot

    # normalize the regional_id from 402 (= 2015) to 400 districts (load_config("base_config.yaml")["regional_id_changes_files"])
    normalized_df = normalize_region_ids_rows(data, id_column='regional_id', data_year=2015)

    # make the regional_id the index
    normalized_df.set_index('regional_id', inplace=True)

    return normalized_df


def filter_consumption_data_per_cts_or_industry(consumption_data: pd.DataFrame, cts_or_industry: str):
    """
    Get consumption data for a specific year.
    Dict of industry and cts industry sectors in dict_cts_or_industry_per_industry_sector()

    Args:
        consumption_data (pd.DataFrame): Consumption data df for a specific year
            index: industry_sectors (88)
            columns: regional_ids (400)
        cts_or_industry (str): 'cts' or 'industry'
    """
    # validate the input
    if cts_or_industry not in ['cts', 'industry']:
        raise ValueError("`cts_or_industry` must be 'cts' or 'industry'")


    # get the industry_sectors from the dict_cts_or_industry_per_industry_sector
    industry_sectors = dict_cts_or_industry_per_industry_sector()[cts_or_industry]

    # filter the consumption_data for the wanted industry_sectors
    consumption_data = consumption_data.loc[industry_sectors]

   
    return consumption_data


def calculate_regional_energy_consumption(consumption_data, energy_carrier, year, regional_energy_consumption_jevi, employees_by_industry_sector_and_regional_ids):
    """
    Calculating the regional energy consumption for industry and cts.
    For CTS we are using the employees data.
    For industry we are using the iterative approach.
    
    This is only necessary for industry, for CTS we are using the employees data.

    Returns:
        - totatl consumption per regional_id and industry_sector for all industry_sectors for the given energy_carrier
        pd.DataFrame:
            - index: industry_sectors
            - columns: regional_ids
    """
    # Get the regional energy consumption for the given year

    # validate the inputs
    if year not in range(2000, 2051):
        raise ValueError("`year` must be between 2000 and 2050")
    

    # 0. find and filter for the wanted energy_carrier
    # find the energy_carrier column in the consumption_data and remove all other columns
    # Find and keep only the column containing the substring 'energy_carrier' e.g. 'power[MWh]'
    matching_cols = [col for col in consumption_data.columns if energy_carrier in col]
    if matching_cols:
        consumption_data = consumption_data.loc[:, matching_cols]
    else:
        raise ValueError(f"No column containing '{energy_carrier}' found.")
    # rename the column to 'consumption[MWh]'
    consumption_data.rename(columns={energy_carrier: "consumption[MWh]"}, inplace=True)


    # 1. calculate the specific consumption per employee per industry_sector
    employees_by_WZ = employees_by_industry_sector_and_regional_ids.sum(axis=1)
    specific_consumption_per_employee_per_industry_sector = consumption_data.div(employees_by_WZ, axis=0)
    # rename the column
    specific_consumption_per_employee_per_industry_sector.rename(columns={"consumption[MWh]": "consumption[MWh/employee]"}, inplace=True)
    

    # 2. Splitting the consumption data to industry and cts 
    consumption_data_cts = filter_consumption_data_per_cts_or_industry(specific_consumption_per_employee_per_industry_sector, 'cts')
    consumption_data_industry = filter_consumption_data_per_cts_or_industry(specific_consumption_per_employee_per_industry_sector, 'industry')


    # 3. For the CTS sector we are using the employees data to get the consumption per regional_id
    # - multiply the specific cinsumption now to the employees per region and industry sector to get 
    # the consumption per regional_id and industry_sector
    regional_consumption_data_cts = consumption_data_cts.mul(employees_by_industry_sector_and_regional_ids, axis=0)


    # 4. for the industry sectore we are using the iterative approach
    # call the function to calculate the consumption per regional_id and industry_sector in the iterative approach
    regional_consumption_data_industry = calculate_iteratively_industry_regional_consumption(consumption_data_industry, year, regional_energy_consumption_jevi, employees_by_industry_sector_and_regional_ids, energy_carrier)


    # 5. merge the consumption data for industry and cts
    consumption_data = pd.concat([regional_consumption_data_cts, regional_consumption_data_industry], axis=0)

    #6. recalculate the total consumption per regional_id and industry_sector
    consumption_data = consumption_data.mul(employees_by_industry_sector_and_regional_ids, axis=0)

    return consumption_data




def calculate_iteratively_industry_regional_consumption(spez_consumption_data_industry, year, regional_energy_consumption_jevi, employees_by_industry_sector_and_regional_ids, energy_carrier):
    """
    Resolves the the consumption per industry_sector (from UGR) to regional_ids (with the help of JEVI) in an iterative approach.


        Args:
            spez_consumption_data_industry: pd.DataFrame with consumption data industry sectors based on the UGR: specific consumption per industry_sector
            year: int, year to calculate the regional energy consumption for
            regional_energy_consumption_jevi: pd.DataFrame with regional energy consumption from JEVI: consumption per regional_id
            employees_by_industry_sector_and_regional_ids: pd.DataFrame with employees by industry_sector and regional_id
            energy_carrier: str, energy carrier to calculate the consumption for: [power, gas, petrol]

        Returns:
            pd.DataFrame:
                - index: industry_sectors
                - columns: regional_ids
    """
    # validate the input
    if energy_carrier not in ['power', 'gas', 'petrol']:
        raise ValueError("`energy_carrier` must be 'power', 'gas' or 'petrol'")
    if year not in range(2000, 2051):
        raise ValueError("`year` must be between 2000 and 2050")
    if (spez_consumption_data_industry.shape[1] != 1):
        raise ValueError("spez_consumption_data_industry must have exactly one column.")
    if not pd.api.types.is_numeric_dtype(spez_consumption_data_industry.index):
        raise TypeError("spez_consumption_data_industry Index must be numeric.")
    if (regional_energy_consumption_jevi.shape[1] < 400) & (regional_energy_consumption_jevi.shape[0] != 1):
        raise ValueError("regional_energy_consumption_jevi must have at least 400 columns and only one row.")
    #if (employees_by_industry_sector_and_regional_ids.shape[1] != ) & (employees_by_industry_sector_and_regional_ids.shape[0] != 1):
    #    raise ValueError("employees_by_industry_sector_and_regional_ids must have at least 400 columns and only one row.")
    


    # TODO iterativer prozess



    return spez_consumption_data_industry




    

