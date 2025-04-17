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
