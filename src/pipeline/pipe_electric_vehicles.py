
import pandas as pd
from src.data_access.local_reader import *
from src.data_processing.electric_vehicles import *


def historical_electric_vehicle_consumption(year: int) -> pd.DataFrame:
    """
    Loads the registered electric vehicles by regional id for the given year.
    """

    # 0. validate input
    if 2000 < year <= 2050:
        raise ValueError("Year must be between 2000 and 2050, year is " + str(year))

    # 1. load data
    number_of_registered_evs = registered_electric_vehicles_by_regional_id(year=year)

    avg_km_per_ev = calculate_avg_km_by_car(year=year)

    avg_kwh_per_km = calculate_avg_kwh_per_km()


    # 2. calculate consumption
    ev_consumption = calculate_electric_vehicle_consumption(number_of_registered_evs=number_of_registered_evs, avg_km_per_ev=avg_km_per_ev, avg_kwh_per_km=avg_kwh_per_km)


    return ev_consumption 
