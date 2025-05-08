
import pandas as pd
from src.data_access.local_reader import *
from src.data_processing.electric_vehicles import *
from src.configs.mappings import *
from src.configs.config_loader import load_config


LAST_YEAR_EXISTING_DATA = load_config()["last_year_existing_registration_data"]


def historical_electric_vehicle_consumption(year: int) -> pd.DataFrame:
    """
    Loads the registered electric vehicles by regional id for the given year in the past

    Args:
        year: int
            Year to load the data for

    Returns:
        pd.DataFrame
            DataFrame with the registered electric vehicles by regional id for the given year
            Columns:
                - regional_id: int
                    The regional id
                - number_of_registered_evs: float
                    The number of registered electric vehicles
    """

    # 0. validate input
    if year < 2007 or year > LAST_YEAR_EXISTING_DATA:
        raise ValueError(f"Year must be between 2007 and {LAST_YEAR_EXISTING_DATA} but is {year}")

    # 1. load data
    number_of_registered_evs = registered_electric_vehicles_by_regional_id(year=year)

    avg_km_per_ev = calculate_avg_km_by_car(year=year)

    avg_mwh_per_km = calculate_avg_mwh_per_km()


    # 2. calculate consumption
    ev_consumption = calculate_electric_vehicle_consumption(number_of_registered_evs=number_of_registered_evs, avg_km_per_ev=avg_km_per_ev, avg_mwh_per_km=avg_mwh_per_km)


    return ev_consumption 


def future_1_electric_vehicle_consumption(year: int) -> pd.DataFrame:
    """
    xxx

    Political Target by the german government: 15mio Electric vehicles by 2030.
    """

    # 0. validate input
    if year < LAST_YEAR_EXISTING_DATA or year > 2045:
        raise ValueError("Year must be between 2000 and 2050, year is " + str(year))
    
    # 1. load data
    existing_ev_stock = calculate_existing_ev_stock(year=LAST_YEAR_EXISTING_DATA)

    total_existing_car_stock = get_total_car_stock()


    # 2. calculate the new number of electric vehicles in the year
    # political target: 15mio electric vehicles by 2030
    number_of_evs = ev_stock_15mio_by_2030(year=year, baseline_year=LAST_YEAR_EXISTING_DATA, baseline_ev=existing_ev_stock, total_stock=total_existing_car_stock)


    # 3. dissaggregate the total consumption into region_ids
    # 3.1 get normalized ev distribution by region
    ev_distribution_by_region = normalized_ev_distribution_by_region(year=LAST_YEAR_EXISTING_DATA)

    # 3.2 get the number of evs by region for the given future year
    number_of_evs_by_region = number_of_evs * ev_distribution_by_region
    # rename the column "ev_share" to "number_of_evs"
    number_of_evs_by_region = number_of_evs_by_region.rename(columns={"ev_share": "number_of_registered_evs"})



    # 3. load consumption data
    avg_km_per_ev = calculate_avg_km_by_car(year=year)

    avg_mwh_per_km = calculate_avg_mwh_per_km()

    # 3. calculate the consumption
    ev_consumption = calculate_electric_vehicle_consumption(number_of_registered_evs=number_of_evs_by_region, avg_km_per_ev=avg_km_per_ev, avg_mwh_per_km=avg_mwh_per_km)


    return ev_consumption


