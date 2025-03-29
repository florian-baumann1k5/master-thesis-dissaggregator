import pandas as pd
import os
from collections import defaultdict


from src import logger
from src.utils.utils import fix_region_id
from src.configs.config_loader import load_config
from src.data_access.api_reader import get_historical_employees, get_future_employees
from src.data_access.local_reader import load_activity_driver_employees
from src.data_processing.normalization import normalize_region_ids_columns
from src.configs.mappings import wz_dict

def get_historical_employees_by_industry_sector_and_regional_id(year, force_preprocessing=False):
    """
    This function returns a dataframe with the number of employees by industry code and regional code.
    Normalization to 400 regions is done in the normalization.py file.
    Basis scenario.
    Years 2000 to 2018 are available.
    WZ35 is empty for all years.


    Args:
        year: The year for which to fetch data
        force_preprocessing: Whether to force preprocessing of the data
        
    Returns:
        A dataframe with the number of employees by industry code and regional code
    
    
    """


    # 1. Validate year
    if year < 2000 or year > 2018:
        raise ValueError(f"No historical employee data available for year {year}")
    elif year >= 2000 and year <= 2008:
        logger.info(f"No historical employee data available for year {year}, using 2008 instead")
        year = 2008

    # 2. Load config
    config = load_config("base_config.yaml")
    processed_dir = config["employees_processed_dir"]
    filename_pattern = config["employees_preprocessed_filename_pattern"]

    # 3. Construct file path
    file_name = filename_pattern.format(year=year)
    preprocessed_file_path = f"{processed_dir}/{file_name}"

    # 4. Check if file exists and force_preprocessing is False
    if not force_preprocessing and os.path.exists(preprocessed_file_path):
        return pd.read_csv(preprocessed_file_path, index_col=0)

    ## Preprocessing needed
    # Load raw data
    df = get_historical_employees(year)
    """
    72180 rows x 8 columns
    """

    # Fix region IDs
    df["id_region"] = df["id_region"].apply(fix_region_id)
    
    # remove negative industry codes
    df = df[df["internal_id[1]"] >= 0]
    """
    70576 rows x 8 columns
    """

    # remove all rows where internal_id[0] is not 9 -> month of observation
    df = df[df["internal_id[0]"] == 9]
    """
    35288 rows x 8 columns
    """

    # Pivot the dataframe
    pivoted_df = df.pivot(
        index="internal_id[1]",
        columns="id_region",
        values="value"
    )
    """
    88 rows x 401 columns
    """
    pivoted_df.index.name = "industry_sector"

    # Optional: Fill missing values with 0
    #pivoted_df = pivoted_df.fillna(0)

    # Normalize region IDs
    pivoted_df = normalize_region_ids_columns(
        df=pivoted_df,
        dataset_year=2018 # is the year of the dataset 401 columns = between 2016 and 2020
        )
    """
    88 rows x 400 columns
    """

    ## Validity check: 
    # the sum().sum() must be between 20mio and 80mio
    if pivoted_df.sum().sum() < 20000000 or pivoted_df.sum().sum() > 80000000:
        raise ValueError(f"Validity check failed: Number of employees: {pivoted_df.sum().sum()}. Must be between 20mio and 80mio")
    
    ## Save to CSV
    os.makedirs(processed_dir, exist_ok=True)
    pivoted_df.to_csv(preprocessed_file_path)

    # Return
    return pivoted_df


def get_future_employees_by_industry_sector_and_regional_id(year, force_preprocessing=False):
    """
    This function returns a dataframe with the number of employees by industry code and regional code.
    Normalization to 400 regions is done in the normalization.py file.
    Future scenario.
    Years 2020 to 2050 are available.
    WZ35 is empty for all years.


    Args:
        year: The year for which to fetch data must be 2018-2050
        force_preprocessing: Whether to force preprocessing of the data
        
    Returns:
        A dataframe with the number of employees by industry code and regional code
    
    
    """

    # 1. Validate year
    year_requested = year
    if year < 2018 or year > 2050:
        raise ValueError(f"No future employee data available for year {year}. Must be between 2018 and 2050. Below 2018 data is provided by get_historical_employees_by_industry_sector_and_regional_id")
    elif year >= 2030:
        # using 2030 as this is the year of the activity drivers
        logger.info(f"No future employee data available for year {year}, using 2030 instead")
        year = 2030 # 2030 is the last year for which data is available

    # 2. Load config
    config = load_config("base_config.yaml")
    processed_dir = config["employees_processed_dir"]
    filename_pattern = config["employees_preprocessed_filename_pattern"]

    # 3. Construct file path
    file_name = filename_pattern.format(year=year_requested)
    preprocessed_file_path = f"{processed_dir}/{file_name}"

    # 4. Check if file exists and force_preprocessing is False
    if not force_preprocessing and os.path.exists(preprocessed_file_path):
        return pd.read_csv(preprocessed_file_path, index_col=0)

    ## Preprocessing needed
    # Load raw data
    df = get_future_employees(year)

    # Fix region IDs
    df["id_region"] = df["id_region"].apply(fix_region_id)
    
    # Pivot the dataframe
    pivoted_df = df.pivot(
        index="internal_id[0]",
        columns="id_region",
        values="value"
    )
    pivoted_df.index.name = "industry_sector"
    """
    88 rows x 401 columns
    """

    pivoted_df = normalize_region_ids_columns(
        df=pivoted_df,
        dataset_year=2018 # is the year of the dataset 401 columns = between 2016 and 2020
    )
    """
    88 rows x 400 columns
    """

    # projecting the data further into the future with the help of the activity driver data
    if year == 2030:
        # load activity driver data
        emp_total = load_activity_driver_employees()
        # multiply each column by its corresponding scaling factor (from the normalized projection for the specified year).
        pivoted_df = (pivoted_df.T.multiply(emp_total.loc[year_requested])).T

    ## Validity check: 
    # the sum().sum() must be between 20mio and 80mio
    if pivoted_df.sum().sum() < 20000000 or pivoted_df.sum().sum() > 80000000:
        raise ValueError(f"Validity check failed: Number of employees: {pivoted_df.sum().sum()}. Must be between 20mio and 80mio")


    # Save to CSV
    os.makedirs(processed_dir, exist_ok=True)
    pivoted_df.to_csv(preprocessed_file_path)

    # Return
    return pivoted_df


def get_employees_per_industry_sector_groups_and_regional_ids(year):
    """
    Wrapper of the functions:
        get_historical_employees_by_industry_sector_and_regional_id(year)
        get_future_employees_by_industry_sector_and_regional_id(year)
    Deciding on the correct function based on the year.
    Grouping the result by industry sector groups of wz_dict().


    Args:
        year: 2000-2050
        
    Returns:
        A dataframe with the number of employees by industry code REGIONS and regional code
        -> 400 rows (regions) x 48 columns (industry sectors)
    
    """

    # 1. Validate year: must be between 2000 and 2050
    if year < 2000 or year > 2050:
        raise ValueError(f"No employee data available for year {year}. Must be between 2000 and 2050.")
    
   
    # 2. select the correct fucntion
    if year >= 2000 and year <= 2018:
        df_emp = get_historical_employees_by_industry_sector_and_regional_id(year)
    elif year >= 2018 and year <= 2050:
        df_emp = get_future_employees_by_industry_sector_and_regional_id(year)

    # 3. translate industry sectors from openffe to our industry sectors and group the data based on the wz_dict groups
    # Transpose so regions are rows, industry_sector codes are columns
    df_employees = df_emp.transpose()
    # Get mapping: {original_industry_sector: group_label}
    mapping = wz_dict()
    # Create reverse mapping: {group_label: [original_industry_sector_codes]}
    group_to_industry_sector = defaultdict(list)
    for industry_sector, group in mapping.items():
        group_to_industry_sector[group].append(industry_sector)
    
    # For each grouped label, sum the corresponding columns
    df_employees_grouped = pd.DataFrame(index=df_employees.index)
    for group_label, industry_sector_list in group_to_industry_sector.items():
        # Filter only those industry_sector codes that actually exist in df_emp
        valid_industry_sector = [industry_sector for industry_sector in industry_sector_list if industry_sector in df_emp.index]
        if valid_industry_sector:
            df_employees_grouped[group_label] = df_employees[valid_industry_sector].sum(axis=1)
        else:
            df_employees_grouped[group_label] = 0.0  # or np.nan if you prefer

    return df_employees_grouped


def get_employees_per_industry_sector_and_regional_ids(year):
    """
    Wrapper of the functions:
        get_historical_employees_by_industry_sector_and_regional_id(year)
        get_future_employees_by_industry_sector_and_regional_id(year)
    Deciding on the correct function based on the year.

    Args:
        year: 2000-2050
        
    Returns:
        A dataframe with the number of employees by industry code and regional code
        -> 88 rows (industry sectors) x 400 columns (regions)
    
    """

    # 1. Validate year: must be between 2000 and 2050
    if year < 2000 or year > 2050:
        raise ValueError(f"No employee data available for year {year}. Must be between 2000 and 2050.")
    
   
    # 2. select the correct fucntion
    if year >= 2000 and year <= 2018:
        df_emp = get_historical_employees_by_industry_sector_and_regional_id(year)
    elif year >= 2018 and year <= 2050:
        df_emp = get_future_employees_by_industry_sector_and_regional_id(year)
   
    return df_emp