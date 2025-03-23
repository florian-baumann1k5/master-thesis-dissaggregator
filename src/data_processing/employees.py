import pandas as pd
import os
from src.data_access.api_reader import get_historical_employees
from src.configs.config_loader import load_config
from src.data_processing.normalization import normalize_region_ids_columns


def get_employees_by_industry_code_and_regional_code(year, force_preprocessing=False):
    # 1. Validate year
    if year < 2000 or year > 2050:
        raise ValueError("Year must be between 2000 and 2050.")

    # 2. Load config
    config = load_config("base_config.yaml")
    processed_dir = config["employees_processed_dir"]
    filename_pattern = config["employees_preprocessed_filename_pattern"]
    target_year = config["employees_target_year"]

    # 3. Construct file path
    file_name = filename_pattern.format(year=year)
    preprocessed_file_path = f"{processed_dir}/{file_name}"

    # 4. Check if file exists and force_preprocessing is False
    if not force_preprocessing and os.path.exists(preprocessed_file_path):
        return pd.read_csv(preprocessed_file_path, index_col=0)

    # 5. Preprocessing needed
    #    A) Load raw data
    df = get_historical_employees(year)
    """
    72180 rows x 8 columns
    """

    #    B) Fix region IDs
    def fix_region_id(rid):
        rid = str(rid)
        if len(rid) == 7:
            rid = "0" + rid  # now 8 chars
        return rid[:-3]     # remove last 3 chars
    
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

    #    D) Normalize region IDs
    pivoted_df = normalize_region_ids_columns(
        df=pivoted_df,
        dataset_year=2018 # is the year of the dataset 401 columns = between 2016 and 2020
        )
    """
    88 rows x 400 columns
    """

    # 6. Save to CSV
    os.makedirs(processed_dir, exist_ok=True)
    pivoted_df.to_csv(preprocessed_file_path)

    # 7. Return
    return pivoted_df

