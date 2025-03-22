import os
import pandas as pd
from src import logger



def get_ugr_data(year, force_preprocessing=False):
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
    
    # 2. Define File Paths
    processed_dir = "data/processed/ugr/"
    processed_file = os.path.join(processed_dir, f"ugr_preprocessed_{year}.csv")
    raw_file = "data/raw/dimensionless/ugr_2000to2020.csv"
    mapping_file = "src/configs/genisis_wz_dict.csv"
    
    # 3. Check for Existing Preprocessed File and force_preprocessing
    # if force_preprocessing is True, the file will be preprocessed again
    if not force_preprocessing and os.path.exists(processed_file):
        return pd.read_csv(processed_file, index_col="industry")
    

    ## Preprocessing the raw data
    logger.info(f"Preprocessing the UGR raw data for year {year}")
    # 4. Load the Raw Data, remove unneeded columns
    raw_data = pd.read_csv(raw_file, delimiter=';')
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
    
    # 6. Remove Rows with Missing Industry Codes and energy carrier codes
    # = removing the "Insgesamt" rows with the sums of energy usage per industry/energy carrier
    year_data = year_data.dropna(subset=["2_variable_attribute_code", "3_variable_attribute_code"])
    
    # 7. Map Industry (=WZ) Codes Using the Mapping File
    mapping_df = pd.read_csv(mapping_file)
    # Create mapping dictionary from Genisis_WZ to WZ
    code_mapping = dict(zip(mapping_df["genisis_industry_code"], mapping_df["industry_code_ranges"]))
    # Apply the mapping in the new column "industry"
    year_data["industry"] = year_data["2_variable_attribute_code"].map(code_mapping)
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
    # Group by industry and energy_type, then sum the values = summing up the energy usage per industry and energy carrier
    grouped_data = year_data.groupby(["industry", "energy_type"])["value"].sum().unstack()
    """ grouped_data:
    48 rows x 3 columns
    industry (=index), power, gas, petrol
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
    industry (=index), power, gas, petrol
    """
    return result_df




