import pandas as pd
from src import logger
import os

from src.configs.config_loader import load_config
from src.data_access.local_reader import load_preprocessed_ugr_file_if_exists, load_raw_ugr_data, load_genisis_wz_sector_mapping_file


import holidays
import datetime
from collections import OrderedDict
from collections.abc import Iterable


def generate_specific_consumption_per_branch(year=2018, **kwargs):
    """
    Returns specific power and gas consumption per branch and year.
        2001-2019:  data from openFfE API
        2018:       data from local pre-calculated data
        2019-2050:  data from local pre-calculated data (2019). !Projecting future consumption using activity drivers from input files

    Also returns total power and gas consumption per branch.
    Also returns the amount of workers per branch and district.
    Also returns the factor of power and gas self generation per branch.

    Returns
    ------------
    dataframe with the following columns:±
    WZ ['power_incl_selfgen[MWh]', 'employees', 'factor_power_no_selfgen', 'gas_incl_selfgen[MWh]', 'factor_gas_no_selfgen']
    88 rows x 5 columns
    """
    logger.info('Function call:', extra={'function': 'generate_specific_consumption_per_branch_flo', 'kwargs': kwargs})

    
    # get a year - if not in kwargs, get it from config
    cfg = kwargs.get('cfg', load_config("base_config.yaml"))
    year = kwargs.get('year', cfg['base_year'])
    # get electricity and gas consumption from database
    x = True
    year1 = year
    # input validation
    if year1 not in range(2000, 2051):
        raise ValueError("`year` must be between 2000 and 2050")
    
    ## get consumption data
    consumption_by_wz = get_total_gas_power_consumption_by_wz(year=year)
    """ consumption_by_wz_ranges:
    WZ  gas[MWh]    power[MWh]
    88 rows x 2 columns
    """
    


    ## calculate specific consumption 
    # spez consumption = consumption / number of employees
    # see Diss S. 73

    employees_by_WZ_and_regional_code = pd.DataFrame(employees_per_branch(year=year))
    """
    88 rows x 401 columns
    """

    # sum up the columns of employees_by_WZ_and_regional_code to get total employees per WZ
    employees_by_WZ = employees_by_WZ_and_regional_code.sum(axis=1).rename("employees")
    """
    88 rows x 1 column
    """

    # merge the employees_by_WZ with the consumption_by_wz
    consumption_and_employees_by_WZ = consumption_by_wz.merge(employees_by_WZ, left_index=True, right_index=True)
    """
    88 rows x 3 column
    columns: gas[MWh] power[MWh] employees
    index: WZ
    """
    
    

    ## add self generation to the consumption_and_employees_by_WZ
    # with the help of the decomposition factors

    # get the decomposition factors
    file_name = "Decomposition Factors.xlsx"
    df_decom_el = pd.read_excel(data_in("dimensionless", file_name), sheet_name="Endenergieverbrauch Strom" )
    df_decom_el.set_index("WZ", inplace=True)
    df_decom_gas = pd.read_excel(data_in("dimensionless", file_name), sheet_name="Endenergieverbrauch Gas" )
    df_decom_gas.set_index("WZ", inplace=True)
    # create new DataFrame with columns from df_decom_el and df_decom_gas
    decomposition_factors_by_WZ = pd.concat(
        [
            df_decom_el[["Strom Netzbezug", "Strom Eigenerzeugung"]],
            df_decom_gas["Anteil Erdgas am Verbrauch aller Gase"],
        ],
        axis=1,
    )
    # fill missing values with 0 and 1 (WZ 35)
    decomposition_factors_by_WZ["Strom Eigenerzeugung"].fillna(0, inplace=True)
    decomposition_factors_by_WZ.fillna(1, inplace=True)


    ## add filter to only look at "Erdgas", ignore the other gases
    consumption_and_employees_by_WZ['gas[MWh]'] = (consumption_and_employees_by_WZ['gas[MWh]'] * decomposition_factors_by_WZ['Anteil Erdgas am Verbrauch aller Gase'])


    # merge the decomposition factors with the consumption_and_employees_by_WZ
    consumption_and_employees_and_decomfactors_by_WZ = consumption_and_employees_by_WZ.merge(decomposition_factors_by_WZ, left_index=True, right_index=True)
    """ consumption_and_employees_and_decomfactors_by_WZ
    WZ ['gas[MWh]', 'power[MWh]', 'employees', 'Strom Netzbezug', 'Strom Eigenerzeugung', 'Anteil Erdgas am Verbrauch aller Gase']
    88 rows x 6 columns
    """





    ## Get the gas consumption for self generation from local files
    # original source (table_id = 71) does not include gas consumption for
    # self generation in industrial sector
    # get gas consumption for self_generation from German energy balance JEVI
    gas_industry_self_consuption = get_gas_industry_self_consuption(year)



    ## Include the power and gas self generation
    # based on the power generation from self generation
    # I have the total gas self gen value but not how it is distributed across the WZs
    # I calculate the factor: How much of the total power self generation is in each WZ 
    # and assume that I can use that factor also for gas

    # self gen is only missing for gas, we get that consumption from JEVI. For power it is already included



    temp_df = consumption_and_employees_and_decomfactors_by_WZ.copy()

    # rename column sincel power already includes selfgen and gas does not
    temp_df.rename(columns={"power[MWh]": "power_incl_selfgen[MWh]", "gas[MWh]": "gas_no_selfgen[MWh]"}, inplace=True)

    temp_df["power_only_selfgen[MWh]"] = temp_df["Strom Eigenerzeugung"] * temp_df["power_incl_selfgen[MWh]"]
    temp_df["factor_selfgen_of_total_power"] = temp_df["power_only_selfgen[MWh]"] / temp_df["power_only_selfgen[MWh]"].sum()

    # use power factor on gas total consumption to get gas self generation per wz
    temp_df["gas_only_selfgen[MWh]"] = temp_df["factor_selfgen_of_total_power"] * gas_industry_self_consuption
    temp_df["gas_incl_selfgen[MWh]"] = temp_df["gas_no_selfgen[MWh]"] + temp_df["gas_only_selfgen[MWh]"]
   

    temp_df["factor_gas_no_selfgen"] = temp_df["gas_no_selfgen[MWh]"] / temp_df["gas_incl_selfgen[MWh]"]
    temp_df.rename(columns={"Strom Netzbezug": "factor_power_no_selfgen"}, inplace=True)


    ## dataframe cleanup
    consumption_employees_no_selfgen_by_WZ = temp_df.copy()
    consumption_employees_no_selfgen_by_WZ.drop(columns=['Strom Eigenerzeugung', 
                                                                   'Anteil Erdgas am Verbrauch aller Gase', 
                                                                   'power_only_selfgen[MWh]',
                                                                   'factor_selfgen_of_total_power', 
                                                                   'gas_only_selfgen[MWh]',
                                                                   'gas_no_selfgen[MWh]'], inplace=True)
    """
    WZ ['power_incl_selfgen[MWh]', 'employees', 'factor_power_no_selfgen', 'gas_incl_selfgen[MWh]', 'factor_gas_no_selfgen']
    88 rows x 5 columns
    """

    # Set the index name to "WZ" before returning
    consumption_employees_no_selfgen_by_WZ.index.name = "WZ"
    return consumption_employees_no_selfgen_by_WZ



def get_ugr_data_ranges(year, force_preprocessing=False):
    """
    Get UGR (Underlying Energy Requirements) data for a specific year. 
    
    Args:
        year (int): The year to filter data for (between 2000 and 2050)
        force_preprocessing (bool): If True, always preprocess the data even if a processed file exists
    
    Returns:
        pandas.DataFrame: The preprocessed UGR data for the specified year
    """
    logger.info(f"src.data_access.local_reader.get_ugr_data: Getting UGR data for year {year}")


    # 1. Validate the Year
    if not 2000 <= year <= 2050:
        raise ValueError(f"Year {year} is outside the valid range (2000-2050)")


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
        "power[TJ]": "power[Mwh]",
        "gas[TJ]": "gas[Mwh]",
        "petrol[TJ]": "petrol[Mwh]"
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
    ordered_columns = ["power[Mwh]", "gas[Mwh]", "petrol[Mwh]"]
    result_df = grouped_data[ordered_columns]
    

    # 12. Save the Preprocessed Data
    # Create directory if it doesn't exist
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


def resolve_industry_sector_ranges(ugr_data: pd.DataFrame) -> pd.DataFrame:
    """
    Resolve the industry_sector ranges in the UGR data.
    """

    
    return ugr_data

