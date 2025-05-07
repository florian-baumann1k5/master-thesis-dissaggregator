import pandas as pd
from src.data_access.local_reader import *

def calculate_electric_vehicle_consumption(number_of_registered_evs: pd.DataFrame, avg_km_per_ev: int, avg_kwh_per_km: float) -> pd.DataFrame:
    """
    Calculate the consumption of electric vehicles.
    """

    # 1. calculate the total consumption per ev
    avg_consumption_per_ev_per_year_in_kwh = avg_km_per_ev * avg_kwh_per_km

    # 2. calculate the total consumption
    ev_consumption = avg_consumption_per_ev_per_year_in_kwh * number_of_registered_evs

    # 3. rename the column in the dataframe
    ev_consumption.rename(columns={'number_of_registered_evs': 'ev_consumption[kwh]'}, inplace=True)

    # 4. check for nan and 0 values
    if ev_consumption.isnull().any().any():
        raise ValueError("NaN values found in ev total consumption")
    if ev_consumption.eq(0).any().any():
        raise ValueError("0 values found in ev total consumption")

    # 4. return the dataframe
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


def calculate_avg_kwh_per_km() -> int:
    """
    Calculate the average kwh per km for the given year.

    Assumption from source [notion 30]: avg consumption is 0.21 kwh/km
    """

    # assumption from source: avg consumption is 0.21 kwh/km

    avg_kwh_per_km = 0.21

    return avg_kwh_per_km

def registered_electric_vehicles_by_regional_id(year: int) -> pd.DataFrame:
    """
    Load the registered electric vehicles by regional id for the given year.

    Data from sourece is only available from the years 2017-2024
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