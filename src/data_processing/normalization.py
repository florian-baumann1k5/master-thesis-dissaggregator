import pandas as pd
from pathlib import Path
from src.configs.config_loader import load_config


def normalize_region_ids_rows(df, id_column, data_year, target_year=None):
    """
    NEW
    Normalize region IDs and sum up values for merged regions.
    Only applies changes forward in time (from data_year to target_year).
    Only applies to rows and only for dfs where with unique region ids.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input dataframe containing region IDs
    id_column : str
        Name of the column containing region IDs
    data_year : int
        Year of the data (source year)
    target_year : int
        Year to normalize the region IDs to (target year)
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame with updated region IDs and summed values
    """

    # 1. Load config
    config = load_config("base_config.yaml")
    regional_id_changes_files = config["regional_id_changes_files"]
    if target_year is None:
        target_year = config["year_for_regional_normalization"]

    # Make a copy to avoid modifying the original dataframe
    df = df.copy()
    
    # Convert region IDs to strings
    df[id_column] = df[id_column].astype(str).str.zfill(5)
    
    # If years are the same or trying to go backwards, return original
    if data_year >= target_year:
        return df
    
    # Apply changes for each year from data_year to target_year-1
    for year in range(data_year, target_year):
        change_file = Path(regional_id_changes_files.format(year=year, year_next=year+1))
        
        if change_file.exists():
            # Read changes for this year
            changes = pd.read_csv(change_file)
            
            # Create mapping dictionary - ensure both old and new AGS codes maintain leading zeros
            mapping = dict(zip(changes['old_ags_lk'].astype(str).str.zfill(5), 
                             changes['new_ags_lk'].astype(str).str.zfill(5)))
            
            # Apply mapping
            df[id_column] = df[id_column].map(lambda x: mapping.get(x, x))
    
    # Sum up all numeric columns for duplicate region IDs
    df = df.groupby(id_column).sum().reset_index()
    
    return df

def normalize_region_ids_columns(df, dataset_year, target_year=None):
    """
    Normalize region IDs in column names by applying yearly mapping changes and combining
    columns that map to the same region ID.

    Parameters:
    -----------
    df : pandas.DataFrame
        Input dataframe where column names are region IDs
    dataset_year : int
        Year of the data (source year)
    target_year : int
        Year to normalize the region IDs to (target year)

    Returns:
    --------
    pandas.DataFrame
        DataFrame with updated column names where columns mapping to the same region ID
        have been summed together
    """
    # 1. Load config
    config = load_config("base_config.yaml")
    regional_id_changes_files = config["regional_id_changes_files"]
    if target_year is None:
        target_year = config["year_for_regional_normalization"]

    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # If years are the same or trying to go backwards, return original
    if dataset_year >= target_year:
        return df

    # Get current column names (region IDs)
    column_ids = df.columns.astype(str).str.zfill(5)
    id_mapping = dict(zip(column_ids, column_ids))  # Initialize with identity mapping

    # Apply changes for each year from dataset_year to target_year-1
    for year in range(dataset_year, target_year):
        change_file = Path(regional_id_changes_files.format(
            year=year,
            year_next=year + 1
        ))

        if change_file.exists():
            # Read changes for this year
            changes = pd.read_csv(change_file)

            # Create mapping dictionary - ensure both old and new AGS codes maintain leading zeros
            year_mapping = dict(zip(
                changes['old_ags_lk'].astype(str).str.zfill(5),
                changes['new_ags_lk'].astype(str).str.zfill(5)
            ))

            # Update the cumulative mapping
            id_mapping = {
                old_id: year_mapping.get(current_id, current_id)
                for old_id, current_id in id_mapping.items()
            }

    # Rename columns using the final mapping
    df.columns = [id_mapping.get(col.zfill(5), col.zfill(5)) for col in df.columns.astype(str)]

    # Group and sum columns with the same mapped ID
    df = df.groupby(level=0, axis=1).sum()

    return df


def normalize_region_ids_average(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function only is applied in cop_ts().

    merging the two columns 16063 and 16056 into one column built by averaging the two.

    The data input is from the year 2019 -> 401 regional_ids. but we are now working with 400 regional_ids (year 2021 ff).
    """
    
    
    df[16063] = (df[16063] + df[16056]) / 2
    df = df.drop(columns=[16056])


    return df