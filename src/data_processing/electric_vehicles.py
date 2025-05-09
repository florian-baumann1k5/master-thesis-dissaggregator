import pandas as pd
from src.data_access.local_reader import *

def calculate_electric_vehicle_consumption(number_of_registered_evs: pd.DataFrame, avg_km_per_ev: int, avg_mwh_per_km: float) -> pd.DataFrame:
    """
    Calculate the consumption of electric vehicles.
    """

    # 1. calculate the total consumption per ev
    avg_consumption_per_ev_per_year_in_mwh = avg_km_per_ev * avg_mwh_per_km

    # 2. calculate the total consumption
    ev_consumption = avg_consumption_per_ev_per_year_in_mwh * number_of_registered_evs

    # 3. rename the column in the dataframe
    ev_consumption.rename(columns={'number_of_registered_evs': 'ev_consumption[mwh]'}, inplace=True)

    # 4. make the ev_consumption[mwh] column float
    ev_consumption['ev_consumption[mwh]'] = ev_consumption['ev_consumption[mwh]'].astype(float)

    # 5. check for nan and 0 values
    if ev_consumption.isnull().any().any():
        raise ValueError("NaN values found in ev total consumption")
    if ev_consumption.eq(0).any().any():
        raise ValueError("0 values found in ev total consumption")


    return ev_consumption

def calculate_avg_km_by_car(year: int) -> int:
    """
    Calculate the average km by car for the given year.

    Datasource only contains the years 2003-2023.
    """

    # 1. find the year
    if year < 2003:
        year_in_dataset = 2003
    elif year > 2023:
        year_in_dataset = 2023
    else:
        year_in_dataset = year

    # 2. load data
    df = load_avg_km_by_car()

    # 3. get the data
    avg_km_by_car = df.loc[year_in_dataset, 'avg_km_per_ev']


    return avg_km_by_car


def calculate_avg_mwh_per_km() -> int:
    """
    Calculate the average mwh per km for the given year.

    Assumption from source [notion 30]: avg consumption is 0.21 kwh/km
    """

    # assumption from source: avg consumption is 0.21 kwh/km = 0.00021 mwh/km

    avg_mwh_per_km = 0.00021

    return avg_mwh_per_km

def registered_electric_vehicles_by_regional_id(year: int) -> pd.DataFrame:
    """
    Load the registered electric vehicles by regional id for the given year.

    Data from sourece is only available from the years 2017-2024

    Args:
        year: int
            Year to load the data for

    Returns:
        pd.DataFrame
            DataFrame with the registered electric vehicles by regional id
            Columns:
                - number_of_registered_evs: float
                    The number of registered electric vehicles
            Index:
                - regional_id: int
                    The regional id
    """

    # 1. load data
    if year < 2017:
        year_in_dataset = 2017
    elif year > 2024:
        year_in_dataset = 2024
    else:
        year_in_dataset = year


    df = load_registered_electric_vehicles_by_regional_id(year=year_in_dataset)
    return df


def calculate_existing_ev_stock(year: int) -> int:
    """
    Calculate the existing ev stock for the given year.
    """

    # 1. load data
    df = registered_electric_vehicles_by_regional_id(year=year)

    # 2. sum it and make it an integer  
    existing_ev_stock = int(df.sum())

    # 3. return the data
    return existing_ev_stock


def s1_future_ev_stock_15mio_by_2030(year,
             baseline_year=2025,
             baseline_ev=1.6e6,
             total_stock=49e6):
    """
    Estimate EV fleet size (absolute number of vehicles) in Germany for a given year
    between baseline_year and final_year, using piecewise linear interpolation.

    Assumptions:
        1. Political Goal of 15mio EVs by 2030
        2. Political Goal of zero CO2 emmisions by 2045
        3. All fossil fuel cars are transitioning to EVs by 2045
        4. Total car stock is constant at 49mio

    Args:
        year : int
            Year to estimate (must be between baseline_year and YEAR_FINAL inclusive).
        baseline_year : int
            Year corresponding to the baseline_ev value.
        baseline_ev : float
            Number of EVs (absolute) at baseline_year.
        total_stock : float
            Total number of vehicles (EV + non-EV), assumed constant.

    Returns:
        float
            Estimated number of EVs in the given year.
    """

    # 1. define the political goals:
    YEAR_Goal = 2030 
    EV_GOAL = 15e6
    YEAR_FINAL = 2045

    # 2. validate input
    if year < baseline_year or year > YEAR_FINAL:
        raise ValueError(f"Year must be between {baseline_year} and {YEAR_FINAL} (inclusive).")


    # 3. calculate the linear interpolation between today-2030 & 2031-2045
    if year <= YEAR_Goal:
        # interpolate between (baseline_year, baseline_ev) and (YEAR_Goal, EV_GOAL)
        return baseline_ev + (EV_GOAL - baseline_ev) * (year - baseline_year) / (YEAR_Goal - baseline_year)
    else:
        # interpolate between (YEAR_Goal, EV_GOAL) and (YEAR_FINAL, total_stock)
        return EV_GOAL + (total_stock - EV_GOAL) * (year - YEAR_Goal) / (YEAR_FINAL - YEAR_Goal)


def normalized_ev_distribution_by_region() -> pd.DataFrame:
    """
    Load the normalized number of electric vehicles distribution by region for the given year.

    Args:
        -

    Returns:
        pd.DataFrame
            DataFrame with the normalized number of electric vehicles distribution by region
            Columns:
                - regional_id: int
                    The regional id
                - ev_share: float
                    The share of electric vehicles in the region
    """
    year = load_config()["last_year_existing_registration_data"]

    # 1. load total number of registered electric vehicles by region
    evs_by_region = registered_electric_vehicles_by_regional_id(year=year)


    # 2. normalize the data
    ev_share_by_region = evs_by_region["number_of_registered_evs"] / evs_by_region["number_of_registered_evs"].sum()
    ev_share_by_region = ev_share_by_region.to_frame(name="ev_share")


    # 3. check if the sum of the ev_share_by_region is 1
    if not np.isclose(ev_share_by_region.sum(), 1.0):
        raise ValueError("The sum of the ev_share_by_region is not 1")


    return ev_share_by_region


def s2_future_ev_stock(year: int, szenario: str) -> pd.DataFrame:
    """
    Load the future ev stock for the given year and szenario.

    Args:
        year: int
            Year to load the data for
        szenario: str
            Szenario to load the data for

    Returns:
        float
    """

    # 1. load data
    data = load_future_ev_stock_s2()

    # 3. validate scenario
    scenarios = data.columns.tolist()
    if szenario not in scenarios:
        raise ValueError(f"Szenario must be one of {scenarios} but is {szenario!r}")
    
    # 4. validate year bounds
    start_year = data.index.min()
    end_year = data.index.max()
    if year < start_year or year > end_year:
        raise ValueError(f"Year must be between {start_year} and {end_year} but is {year}")
    
    # 5. reindex to every year & interpolate by index (i.e. by the year values)
    full_index = pd.RangeIndex(start_year, end_year + 1, name=data.index.name or "year")
    annual_df = (
        data
        .reindex(full_index)            # insert missing years
        .interpolate(method="index")    # linear interpolation weighted by year gaps
    )
    
   # 6. extract the single-year, single-scenario result
    annual_ev_stock = annual_df.at[year, szenario]


    return annual_ev_stock


def regional_dissaggregation_evs(evs_germany:pd.DataFrame) -> pd.DataFrame:
    """
    Dissaggregate the total number of electric vehicles in Germany to the regional level.

    Args:
        evs_germany: float
        

    Returns:
        pd.DataFrame
            index: regional_id
            columns: number_of_registered_evs per region
    """
    
    # 1. get the normalized regional distribution of EVs 
    ev_distribution_by_region = normalized_ev_distribution_by_region()


    # 2. get the number of evs by region for the given future year
    number_of_evs_by_region = evs_germany * ev_distribution_by_region

    # 3. rename the column "ev_share" to "number_of_evs"
    number_of_evs_by_region = number_of_evs_by_region.rename(columns={"ev_share": "number_of_registered_evs"})


    # 4. validation
    if not np.isclose(number_of_evs_by_region.sum(), evs_germany):
        raise ValueError("The sum of the evs by region is not equal to the total number of evs in Germany")


    return number_of_evs_by_region
    