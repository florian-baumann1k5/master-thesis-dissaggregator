import ast
import pandas as pd
import numpy as np
from datetime import timedelta

from src.configs.mappings import *
from src.utils.utils import *
from src.data_access.api_reader import *
from src.data_processing.normalization import *
from src.data_access.local_reader import *


def allocation_temperature_by_day(year: int, force_preprocessing: bool = False):
    """
    DISS 4.4.3.2 Erstellung von Wärmebedarfszeitreihen
    old function data.t_allo() -> checked, it is the same output as the new function

    Allocate temperature data to a given year based of a historical year on a daily basis.

    Args:
        year: int

    Returns:
        - a dataframe with the temperature outside in daily average resolution for a given year.
            - index is the datetime index daily for the historical year!
            - columns are the regional ids (400)
            - values are the temperature outside in °C
    """

    # 0. validate input
    if year < 2000 or year > 2050:
        raise ValueError(f"Year must be between 2000 and 2050, got {year}")
    

    # 1. check if the data is already in the cache
    if not force_preprocessing:
        daily_temperature_allocation = load_temperature_allocation_cache(year=year, resolution="day")

        if daily_temperature_allocation is not None:
            return daily_temperature_allocation


    # 2. Get the number of 15-minute intervals in the year (takes into account leap years)
    periods = get_15min_intervals_per_year(year)


    # 3. Get the historical year for a future year
    hist_year = hist_weather_year().get(year)


    # 4. Get the temperature outside in hourly resolution for a given year from openffe
    temp_outside_hourly = get_temp_outside_hourly_for_regions(hist_year)


    # 5. Convert the temperature outside in hourly resolution to daily average temperature
    temp_outside_daily_avg = (temp_outside_hourly.assign(date=pd.date_range((str(hist_year) + '-01-01'), periods=periods / 4, freq='h'))
            .set_index('date')
            .resample('D')
            .mean())
    temp_outside_daily_avg = temp_outside_daily_avg[sorted(temp_outside_daily_avg.columns)]


    # 6. Smooth the temperature outside in daily average temperature
    for regional_id in temp_outside_daily_avg.columns:
        te = temp_outside_daily_avg[regional_id].values
        for i in range(len(te)-1, -1, -1):
            if (i >= 3):
                te[i] = ((te[i] + 0.5 * te[i - 1] + 0.25 * te[i - 2]
                         + te[i - 3] * 0.125) / 1.875)
        temp_outside_daily_avg[regional_id] = te


    # 7. save to cache
    processed_dir = load_config("base_config.yaml")['temperature_allocation_cache_dir']
    processed_file = os.path.join(processed_dir, load_config("base_config.yaml")['temperature_allocation_cache_file'].format(year=year, resolution="day"))
    os.makedirs(processed_dir, exist_ok=True)
    temp_outside_daily_avg.to_csv(processed_file)


    return temp_outside_daily_avg


def allocation_temperature_by_hour(year: int, force_preprocessing: bool = False):
    """
    
    old function data.resample_t_allo() -> checked, it is the same output as the new function
    Allocate temperature data to a given year.

    Every temp per day is used as the temp for each hour of that day.

    Args:
        year: int   

    Returns:
        - a dataframe with the temperature outside in hourly resolution for a given year.
        - Index is the datetime index hourly
        - columns are the regional ids
    """

    # 0. validate input
    if year < 2000 or year > 2050:
        raise ValueError(f"Year must be between 2000 and 2050, got {year}")
    

    # 1. check if the data is already in the cache
    if not force_preprocessing:
        hourly_temperature_allocation = load_temperature_allocation_cache(year=year, resolution="hour")

        if hourly_temperature_allocation is not None:
            return hourly_temperature_allocation


    # 2. Get the number of 15-minute intervals in the year (takes into account leap years)
    periods = get_hours_of_year(year)
    date = pd.date_range((str(year) + '-01-01'), periods=periods, freq='h')


    # 3. Get the temperature outside in hourly resolution for a given year from openffe
    dayly_temperature_allocation = allocation_temperature_by_day(year=year, force_preprocessing=True)


    # 4. Add the last hour of the previous day to the dataframe
    dayly_temperature_allocation.index = pd.to_datetime(dayly_temperature_allocation.index)
    last_hour = dayly_temperature_allocation.copy()[-1:]
    last_hour.index = last_hour.index + timedelta(1)
    dayly_temperature_allocation = pd.concat([dayly_temperature_allocation, last_hour])


    # 5. resample
    hourly_temperature_allocation = dayly_temperature_allocation.resample('h').ffill()
    hourly_temperature_allocation = hourly_temperature_allocation[:-1]
    hourly_temperature_allocation.index = date
    hourly_temperature_allocation.columns = hourly_temperature_allocation.columns.astype(int)


    # 6. save to cache
    processed_dir = load_config("base_config.yaml")['temperature_allocation_cache_dir']
    processed_file = os.path.join(processed_dir, load_config("base_config.yaml")['temperature_allocation_cache_file'].format(year=year, resolution="hour"))
    os.makedirs(processed_dir, exist_ok=True)
    hourly_temperature_allocation.to_csv(processed_file)


    return hourly_temperature_allocation





def get_temp_outside_hourly_for_regions(year: int):
    """
    Old function: data.ambient_T()

    Get the temperature outside in hourly resolution for a given year.


    Warning:
        - The API is returing regions that are not in the mapping. This is in particular: 3152, 3156, 16056
        - we are also not normalizing the regions, instead we just filter them 

    Returns:
        - a dataframe with the temperature outside in hourly resolution for a given year.
        - Index is the datetime index 
        - columns are the regional ids (400)
    """

    # 1. validate input
    if year < 2006 or year > 2019:
        raise ValueError(f"No temperature outside data available for year {year}")


    # 2. get the correct his weather year
    hist_year = hist_weather_year().get(year)


    # 3. get data
    df = get_temperature_outside_hourly(hist_year)
    

    # 4. Fix region IDs
    df["id_region"] = df["id_region"].apply(fix_region_id)
    df["id_region"] = df["id_region"].astype(int)


    # 5. drop regions that are not in the mapping
    all_regional_ids = get_all_regional_ids()
    all_regional_ids = all_regional_ids["regional_id"].tolist()
    temp_outside_hourly = df[df["id_region"].isin(all_regional_ids)]


    # 6. validate output length
    expected_rows = len(all_regional_ids)
    actual_rows = len(temp_outside_hourly)
    if actual_rows != expected_rows:
        raise ValueError(f"Row count mismatch: expected {expected_rows}, got {actual_rows}. Check which rows are missing.")
    

    # 7. create datetime index
    expected_hours = get_hours_of_year(year)
    datetime_index = pd.date_range(
        start=f'{year}-01-01 00:00:00',
        periods=expected_hours, # Use periods based on calculated hours
        freq='h' # Hourly frequency
    )

    if len(datetime_index) != expected_hours:
         # This check is mostly for internal consistency of date_range logic
         print(f"Warning: Mismatch between calculated hours ({expected_hours}) and generated index length ({len(datetime_index)}). Using index length.")
         expected_hours = len(datetime_index)

    # 8. Prepare data: Set index and select 'values'
    # Ensure no duplicate regions, keep first if duplicates exist
    if temp_outside_hourly['id_region'].duplicated().any():
        print("Warning: Duplicate 'id_region' found. Keeping the first occurrence.")
        temp_unique_regions = temp_outside_hourly.drop_duplicates(subset=['id_region'], keep='first')
    else:
        temp_unique_regions = temp_outside_hourly

    
    temp_indexed = temp_unique_regions.set_index('id_region')
    temp_series = temp_indexed['values']


    # 9. Process 'values' column (handle strings or existing lists/arrays)
    processed_data = {}
    for region_id, value_data in temp_series.items():
        hourly_list = None
        try:
            if isinstance(value_data, str):
                # Attempt to parse string representation of a list
                parsed_list = ast.literal_eval(value_data)
                if isinstance(parsed_list, list):
                    hourly_list = parsed_list
                else:
                     raise ValueError(f"Parsed data for region {region_id} is not a list.")
            elif isinstance(value_data, (list, pd.Series, np.ndarray)):
                 # Assume it's already a list-like structure
                 hourly_list = list(value_data) # Convert to standard list
            else:
                 raise TypeError(f"Unsupported data type in 'values' for region {region_id}: {type(value_data)}")

            # Validate length
            if len(hourly_list) != expected_hours:
                 raise ValueError(f"Data length mismatch for region {region_id}. "
                                  f"Expected {expected_hours}, found {len(hourly_list)}.")

            processed_data[region_id] = hourly_list

        except (SyntaxError, ValueError, TypeError) as e:
             print(f"Error processing data for region {region_id}: {e}")
             raise ValueError(f"Failed to process 'values' for region {region_id}. Ensure it's a valid list/array (or string representation) with correct length.") from e



    final_df = pd.DataFrame(processed_data, index=datetime_index)

    print(f"Successfully created DataFrame with shape {final_df.shape}")
    return final_df








