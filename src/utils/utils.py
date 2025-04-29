from collections import defaultdict
import pandas as pd
from ast import literal_eval as lit_eval
from src.configs.mappings import *


def fix_region_id(rid):
    rid = str(rid)
    if len(rid) == 7:
        rid = "0" + rid  # now 8 chars
    return rid[:-3]     # remove last 3 chars


def group_industry_sectors(df, mapping_dict=industry_sector_groups()):

    """
    Groups industry sectors in a DataFrame based on predefined ranges in mapping_dict.
    Sums up columns that correspond to ranges (e.g., '10-12' will sum columns '10', '11', '12')
    and removes the original columns that were summed.
    
    Args:
        df (pd.DataFrame): DataFrame with industry sector columns as strings
        
    Returns:
        pd.DataFrame: DataFrame with grouped ranges and remaining individual sectors
    """
    # Create a copy to avoid modifying the original DataFrame
    result_df = df.copy()
    
    # Process each mapping item
    for item in mapping_dict:
        if '-' in item:
            # Handle range
            start_str, end_str = item.split('-')
            try:
                start, end = int(start_str), int(end_str)
                # Find columns within this range
                cols_to_sum = [i for i in range(start, end + 1) if i in df.columns]
                
                if cols_to_sum:
                    # Create new column with sum
                    result_df[item] = df[cols_to_sum].sum(axis=1)
                    # Remove original columns that were summed
                    result_df = result_df.drop(columns=cols_to_sum)
            except ValueError:
                continue


    # Ensure all df column names are strings for comparison
    df_columns_str = [str(col) for col in result_df.columns]

    #Validation
    # Compare each key in mapping_dict to df columns and print result
    # for group_label in mapping_dict:
    #     if group_label in df_columns_str:
    #         print(f"{group_label}: ✅ found in df.columns")
    #     else:
    #         print(f"{group_label}: ❌ NOT found in df.columns")
    
    # industry_sector 35 is missing in the result_df
    return result_df


def get_days_of_year(year: int) -> int:
    """
    Returns the number of days in a given year.
    """
    is_leap = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
    return 366 if is_leap else 365


def get_hours_of_year(year: int) -> int:
    """
    Returns the number of hours in a given year.
    """
    is_leap = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
    hours = 8784 if is_leap else 8760

    return hours


def get_15min_intervals_per_year(year: int) -> int:
    """
    Returns the number of 15-minute periods in a given year.
    35040 for normal years, 35136 for leap years.
    """
    is_leap = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
    return 35136 if is_leap else 35040


def literal_converter(val):
    try:
        return lit_eval(val)
    except (SyntaxError, ValueError):
        return val


def translate_application_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename all columns of `df` according to `mapping`.
    Raises KeyError if any column in df is not present in mapping.
    """

    mapping = translate_application_columns_mapping()
    missing = set(df.columns) - set(mapping)
    if missing:
        raise KeyError(f"No translation provided for columns: {sorted(missing)}")
    
    df = df.rename(columns=mapping)
    return df


def group_activity_drivers(df_driver_total, columns):
    """
    Reshapes activity driver data to match partly aggregated consumption data
    from publications.

    Parameters
    -------
    df_driver_total : pd.DataFrames
        Activity drivers as read from model input. years in index, branches in
        columns
    columns : np.array or index
        Defined aggregation of branches from publication.

    Returns
    -------
    pd.DataFrame

    """
    # create new DF with aggregated drivers
    new_df = pd.DataFrame(index=df_driver_total.index, columns=columns, data=0)
    for i in [1, 2, 3, 5, 6, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
              28, 29, 30, 33, 36, 43, 45, 46, 47, 49, 50, 51, 52, 53, 68,
              84, 85]:
        new_df[str(i)] = df_driver_total[i]
    for i in [7, 8, 9]:
        new_df['7-9'] = new_df['7-9'] + df_driver_total[i]
    for i in [10, 11, 12]:
        new_df['10-12'] = new_df['10-12'] + df_driver_total[i]
    for i in [13, 14, 15]:
        new_df['13-15'] = new_df['13-15'] + df_driver_total[i]
    new_df['31-32'] = (df_driver_total[31] + df_driver_total[32])
    new_df['37-39'] = (df_driver_total[37] + df_driver_total[38] + df_driver_total[39])
    new_df['41-42'] = (df_driver_total[41] + df_driver_total[42])
    new_df['55-56'] = (df_driver_total[55] + df_driver_total[56])
    for i in [58, 59, 60, 61, 62, 63]:
        new_df['58-63'] = new_df['58-63'] + df_driver_total[i]
    for i in [64, 65, 66]:
        new_df['64-66'] = new_df['64-66'] + df_driver_total[i]
    for i in [69, 70, 71, 72, 73, 74, 75]:
        new_df['69-75'] = new_df['69-75'] + df_driver_total[i]
    for i in [77, 78, 79, 80, 81, 82]:
        new_df['77-82'] = new_df['77-82'] + df_driver_total[i]
    for i in [86, 87, 88]:
        new_df['86-88'] = new_df['86-88'] + df_driver_total[i]
    for i in [90, 91, 92, 93, 94, 95, 96, 97, 98, 99]:
        new_df['90-99'] = new_df['90-99'] + df_driver_total[i]

    return new_df.drop('35', axis=1)


def get_regional_ids_by_state(state: str) -> list[int]:
    """
    Args:
        state: two-letter abbreviation of a Bundesland (e.g. 'SH', 'BY', 'NW', etc.)

    Returns:
        A list of all `regional_id` (as ints) from landkreise_2023.csv
        whose first digits correspond to that state.
    """
    from src.data_access.local_reader import get_all_regional_ids

    # 1. Map abbreviation back to its numeric code
    inv = {abbr: num for num, abbr in federal_state_dict().items()}
    state = state.upper()
    if state not in inv:
        raise ValueError(f"Unknown state abbreviation: {state!r}")

    code = inv[state]

    # 2. Load the CSV
    df = get_all_regional_ids()

    # 3. Extract the state-prefix by integer division
    #    (drops the last three digits)
    df["state_code"] = df["regional_id"] // 1000

    # 4. Filter and return the full regional_ids
    mask = df["state_code"] == code
    return df.loc[mask, "regional_id"].tolist()

