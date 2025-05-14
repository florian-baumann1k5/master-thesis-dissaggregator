
import pandas as pd
from src.data_access.local_reader import *
from src.data_processing.electric_vehicles import *
from src.configs.mappings import *
from src.configs.config_loader import load_config


FIRST_YEAR_EXISTING_DATA_KVB = load_config()["first_year_existing_registration_data"]
LAST_YEAR_EXISTING_DATA_KVB = load_config()["last_year_existing_registration_data"]
FIRST_YEAR_EXISTING_DATA_UGR = load_config()["first_year_existing_fuel_consumption_ugr"]
LAST_YEAR_EXISTING_DATA_UGR = load_config()["last_year_existing_fuel_consumption_ugr"]



# Szenario 1 & 2: via KVB -> registered Vehicle stock
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
    if year < FIRST_YEAR_EXISTING_DATA_KVB or year > LAST_YEAR_EXISTING_DATA_KVB:
        raise ValueError(f"Year must be between {FIRST_YEAR_EXISTING_DATA_KVB} and {LAST_YEAR_EXISTING_DATA_KVB} but is {year}")

    # 1. load data
    number_of_registered_evs = registered_electric_vehicles_by_regional_id(year=year)
    avg_km_per_ev = calculate_avg_km_by_car(year=year)
    avg_mwh_per_km = calculate_avg_mwh_per_km()


    # 2. calculate consumption
    ev_consumption_by_region = calculate_electric_vehicle_consumption(data_in=number_of_registered_evs, avg_km_per_ev=avg_km_per_ev, avg_mwh_per_km=avg_mwh_per_km)


    return ev_consumption_by_region 

def future_1_electric_vehicle_consumption(year: int) -> pd.DataFrame:
    """
    TODO: description

    Political Target by the german government: 15mio Electric vehicles by 2030.
    """

    # 0. validate input
    if year < LAST_YEAR_EXISTING_DATA_KVB or year > 2045:
        raise ValueError("Year must be between 2000 and 2050, year is " + str(year))
    
    # 1. load data
    existing_ev_stock = calculate_existing_ev_stock(year=LAST_YEAR_EXISTING_DATA_KVB)
    total_existing_car_stock = get_total_car_stock()


    # 2. calculate the new number of electric vehicles in the year
    # political target: 15mio electric vehicles by 2030
    number_of_evs = s1_future_ev_stock_15mio_by_2030(year=year, baseline_year=LAST_YEAR_EXISTING_DATA_KVB, baseline_ev=existing_ev_stock, total_stock=total_existing_car_stock)


    # 3. load consumption data
    avg_km_per_ev = calculate_avg_km_by_car(year=year)
    avg_mwh_per_km = calculate_avg_mwh_per_km()


    # 4. calculate the consumption
    ev_consumption = calculate_electric_vehicle_consumption(data_in=number_of_evs, avg_km_per_ev=avg_km_per_ev, avg_mwh_per_km=avg_mwh_per_km)


    # 5. dissaggregate the total consumption into region_ids
    ev_consumption_by_region = regional_dissaggregation_ev_consumption(ev_consumption=ev_consumption)


    return ev_consumption_by_region

def future_2_electric_vehicle_consumption(year: int, szenario: str = "trend") -> pd.DataFrame:
    """
    TODO: decription

    Predicted EV market penetration based on different szenarios.

    Args:
        year: int
            Year to load the data for
        szenario: str
            Szenario to load the data for [trend,ambit,regio]

    Returns:
        pd.DataFrame: DataFrame with the registered electric vehicles by regional id for the given year
            Columns:
                - regional_id: int
                    The regional id
                - number_of_registered_evs: float
                    The number of registered electric vehicles
    """

    # 0. validate input
    if year < LAST_YEAR_EXISTING_DATA_KVB or year > 2045:
        raise ValueError("Year must be between {LAST_YEAR_EXISTING_DATA_KVB} and 2045, year is " + str(year))
    if szenario not in ["ambit", "trend", "regio"]:
        raise ValueError("Szenario must be in ['ambit', 'trend', 'regio']")
    
    
    # 1. calculate the new number of electric vehicles in the year
    # political target: 15mio electric vehicles (E-autos, nicht LKW) by 2030
    number_of_evs = s2_future_ev_stock(year=year, szenario=szenario)


    # 2. load consumption data
    avg_km_per_ev = calculate_avg_km_by_car(year=year)
    avg_mwh_per_km = calculate_avg_mwh_per_km()


    # 3. calculate the consumption
    ev_consumption = calculate_electric_vehicle_consumption(data_in=number_of_evs, avg_km_per_ev=avg_km_per_ev, avg_mwh_per_km=avg_mwh_per_km)


    # 4. dissaggregate the total consumption into region_ids
    ev_consumption_by_region = regional_dissaggregation_ev_consumption(ev_consumption=ev_consumption)


    return ev_consumption_by_region


# main function for s1 and s2
def s1_2_electric_vehicle_consumption(year: int, szenario: str, s2_szenario: str) -> pd.DataFrame:
    """
    TODO: description
    """

    # 0. validate input
    if year < FIRST_YEAR_EXISTING_DATA_KVB or year > 2045:
        raise ValueError(f"Year must be between {FIRST_YEAR_EXISTING_DATA_KVB} and 2045 but is {year}")
    
    # 1. load data
    if year <= LAST_YEAR_EXISTING_DATA_KVB:
        ev_consumption_by_region = historical_electric_vehicle_consumption(year=year)
    elif year > LAST_YEAR_EXISTING_DATA_KVB:
        if szenario == "KVB_1":
            ev_consumption_by_region = future_1_electric_vehicle_consumption(year=year)
        elif szenario == "KVB_2":
            ev_consumption_by_region = future_2_electric_vehicle_consumption(year=year, szenario=s2_szenario)


    return ev_consumption_by_region




# Szenario 3: via UGR -> fuel consumption
def s3_electric_vehicle_consumption(year: int) -> pd.DataFrame:
    """
    Loads the registered electric vehicles by regional id for the given year in the past

    Args:
        year: int
            Year to load the data for

    Returns:
        pd.DataFrame: DataFrame with the registered electric vehicles by regional id for the given year
    """

    # 0. validate input
    if year < FIRST_YEAR_EXISTING_DATA_UGR or year > 2045:
        raise ValueError(f"Year must be between {FIRST_YEAR_EXISTING_DATA_UGR} and 2045 but is {year}")


    if year <= LAST_YEAR_EXISTING_DATA_UGR:
        historical_data = get_historical_vehicle_consumption_ugr_by_energy_carrier(year=year)    
        # get the float value as float
        ev_consumption = historical_data["power[mwh]"].values[0]

    elif year > LAST_YEAR_EXISTING_DATA_UGR:
        future_data = get_future_vehicle_consumption_ugr_by_energy_carrier(year=year)
        # get the float value as float
        ev_consumption = future_data["power[mwh]"].values[0]


    # 5. dissaggregate the total consumption into region_ids
    ev_consumption_by_region = regional_dissaggregation_ev_consumption(ev_consumption=ev_consumption)


    return ev_consumption_by_region










# Main function combining s1, s2 and s3
def electric_vehicle_consumption_by_regional_id(year: int, szenario: str, s2_szenario: str = None) -> pd.DataFrame:
    """
    Loads the registered electric vehicles by regional id for the given year in the past
    """

    # 0. validate input
    if szenario == "KVB_1" or szenario == "KVB_2":
        if year < FIRST_YEAR_EXISTING_DATA_KVB or year > 2045:
            raise ValueError(f"Year must be between {FIRST_YEAR_EXISTING_DATA_KVB} and 2045 but is {year}")        
    elif szenario == "UGR":
        if year < FIRST_YEAR_EXISTING_DATA_UGR or year > 2045:
            raise ValueError(f"Year must be between {FIRST_YEAR_EXISTING_DATA_UGR} and 2045 but is {year}")
    else:
        raise ValueError("szenario must be in ['KVB_1', 'KVB_2', 'UGR']")
    if szenario == "KVB_2" and s2_szenario not in ["ambit", "trend", "regio"]:
        raise ValueError("s2_szenario must be in ['ambit', 'trend', 'regio']")


    # 0.1 check the cache
    cache_dir = load_config("base_config.yaml")['electric_vehicle_consumption_by_regional_id_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['electric_vehicle_consumption_by_regional_id_cache_file'].format(year=year, szenario=szenario, s2_szenario=s2_szenario))

    if os.path.exists(cache_file):
        logger.info(f"Load electric_vehicle_consumption_by_regional_id from cache for year: {year}, szenario: {szenario}, s2_szenario: {s2_szenario}")
        return pd.read_csv(cache_file)


    # 1. load the data
    # KVB Szenario
    if "KVB" in szenario:
        ev_consumption_by_region = s1_2_electric_vehicle_consumption(year=year, szenario=szenario, s2_szenario=s2_szenario)
        
    # UGR Szenario
    elif "UGR" in szenario:
        ev_consumption_by_region = s3_electric_vehicle_consumption(year=year)


    # 2. save the data to the cache
    if ev_consumption_by_region.isna().any().any():
        raise ValueError("DataFrame contains NaN values")
    os.makedirs(cache_dir, exist_ok=True)
    logger.info(f"Save electric_vehicle_consumption_by_regional_id to cache for year: {year}, szenario: {szenario}, s2_szenario: {s2_szenario}")
    ev_consumption_by_region.to_csv(cache_file)    

    return ev_consumption_by_region





