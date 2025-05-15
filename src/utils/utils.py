from collections import defaultdict
import pandas as pd
from ast import literal_eval as lit_eval
from src.configs.mappings import *
import holidays

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



def create_weekday_workday_holiday_mask(state: str, year: int) -> pd.DataFrame:
    """
    Creates a DataFrame mask for a given German state and year, indicating
    workdays, weekends, holidays, and the type of day.

    Args:
        state (str): The 2-letter abbreviation for the German state
                          (e.g., "TH" for Thüringen, "BY" for Bayern).
        year (int): The year for which to create the mask.

    Returns:
        pd.DataFrame: A DataFrame with a DatetimeIndex covering all days of
                      the specified year and five columns:
                      - "workday" (bool): True if the day is a standard workday
                                          (not a weekend or holiday).
                      - "weekend" (bool): True if the day is a Saturday or Sunday.
                      - "holiday" (bool): True if the day is a public holiday.
                      - "weekend_holiday" (bool): True if the day is a Saturday,
                                                  Sunday, or a public holiday.
                      - "day" (str): The lowercase name of the day of the week
                                     (e.g., "monday", "tuesday") or "holiday"
                                     if it's a public holiday.
    Raises:
        ValueError: If the provided state is not a valid German state
                    abbreviation recognized by the holidays library, or if the
                    year is invalid.
    """

    # 1. Generate a DatetimeIndex for all days in the specified year
    try:
        start_date = pd.Timestamp(f"{year}-01-01")
        end_date = pd.Timestamp(f"{year}-12-31")
    except ValueError:
        raise ValueError(f"Invalid year provided: {year}")

    dates_in_year = pd.date_range(start=start_date, end=end_date, freq='D')

    # 2. Initialize the DataFrame with the date index
    df = pd.DataFrame(index=dates_in_year)
    df.index.name = 'date'

    # 3. Get public holidays for the specified German state and year
    upper_state = state.upper()
    try:
        german_holidays_obj = holidays.DE(state=upper_state, years=year)
    except KeyError:
        valid_states = holidays.Germany.subdivisions
        raise ValueError(
            f"Invalid German state abbreviation: '{state}'. "
            f"Valid abbreviations are: {valid_states}"
        )

    # 4. Populate the "holiday" column (NEW)
    # True if the day is a public holiday.
    # df.index contains pd.Timestamp; german_holidays_obj contains datetime.date.
    df['holiday'] = [ts.date() in german_holidays_obj for ts in df.index]

    # 5. Determine day of the week and populate "weekend" column (NEW)
    # day_of_week_num: Monday=0, Tuesday=1, ..., Sunday=6
    df['day_of_week_num'] = df.index.dayofweek
    is_saturday = (df['day_of_week_num'] == 5)
    is_sunday = (df['day_of_week_num'] == 6)
    df['weekend'] = is_saturday | is_sunday

    # 6. Populate the "weekend_holiday" column (uses new 'weekend' and 'holiday' columns)
    # True if the day is a Saturday, Sunday, or a public holiday.
    df['weekend_holiday'] = df['weekend'] | df['holiday']

    # 7. Populate the "workday" column
    # A workday is any day that is NOT a weekend or a holiday.
    df['workday'] = ~df['weekend_holiday']

    # 8. Populate the "day" column (uses new 'holiday' column)
    # Contains the lowercase name of the day of the week or "holiday".
    # Holiday status takes precedence.
    df['day'] = df.index.day_name().str.lower()
    df.loc[df['holiday'], 'day'] = 'holiday' # If it's a holiday, mark as 'holiday'

    # 9. Return the DataFrame with the required columns in a specific order
    final_columns = ['workday', 'weekend', 'holiday', 'weekend_holiday', 'day']
    return df[final_columns]