
import pandas as pd
from src.data_access.local_reader import *
from src.data_processing.electric_vehicles import *
from src.configs.mappings import *
from src.configs.config_loader import load_config


FIRST_YEAR_EXISTING_DATA_KBA = load_config()["first_year_existing_registration_data_kba"]
LAST_YEAR_EXISTING_DATA_KBA = load_config()["last_year_existing_registration_data_kba"]
FIRST_YEAR_EXISTING_DATA_UGR = load_config()["first_year_existing_fuel_consumption_ugr"]
LAST_YEAR_EXISTING_DATA_UGR = load_config()["last_year_existing_fuel_consumption_ugr"]



# Szenario 1 & 2: via KBA -> registered Vehicle stock
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
    if year < FIRST_YEAR_EXISTING_DATA_KBA or year > LAST_YEAR_EXISTING_DATA_KBA:
        raise ValueError(f"Year must be between {FIRST_YEAR_EXISTING_DATA_KBA} and {LAST_YEAR_EXISTING_DATA_KBA} but is {year}")

    # 1. load data
    number_of_registered_evs = registered_electric_vehicles_by_regional_id(year=year)
    share_of_commercial_vehicles = share_of_commercial_vehicles_by_regional_id(year=year)
    avg_km_per_ev = calculate_avg_km_by_car(year=year)
    avg_mwh_per_km = calculate_avg_mwh_per_km()

    # validation
    if not number_of_registered_evs.equals(share_of_commercial_vehicles.index):
        raise ValueError("number_of_registered_evs and share_of_commercial_vehicles must have the same index")

    # 2. calculate the number of private vehicles
    number_of_commercial_vehicles = number_of_registered_evs * share_of_commercial_vehicles
    number_of_private_vehicles = number_of_registered_evs - number_of_commercial_vehicles


    # 3. calculate consumption
    ev_consumption_by_region = calculate_electric_vehicle_consumption(data_in=number_of_private_vehicles, avg_km_per_ev=avg_km_per_ev, avg_mwh_per_km=avg_mwh_per_km)


    return ev_consumption_by_region 

def future_1_electric_vehicle_consumption(year: int) -> pd.DataFrame:
    """
    Calculate the future consumption of electric vehicles based on the number of electric vehicles and the average km per ev and the average mwh per km.

    Political Target by the german government: 15mio Electric vehicles by 2030.

    Args:
        year: int
            Year to calculate the future consumption for
    Returns:
        pd.DataFrame
            index: regional_id
            columns: power[mwh]
    """

    # 0. validate input
    if year < LAST_YEAR_EXISTING_DATA_KBA or year > 2045:
        raise ValueError("Year must be between 2000 and 2050, year is " + str(year))
    
    # 1. load data
    share_of_commercial_vehicles_regional_id = share_of_commercial_vehicles_by_regional_id(year=year)
    existing_ev_stock = calculate_existing_ev_stock(year=LAST_YEAR_EXISTING_DATA_KBA)
    total_existing_car_stock = get_total_car_stock()


    # 2. calculate the new number of electric vehicles in the year
    # political target: 15mio electric vehicles by 2030
    number_of_evs = s1_future_ev_stock_15mio_by_2030(year=year, baseline_year=LAST_YEAR_EXISTING_DATA_KBA, baseline_ev=existing_ev_stock, total_stock=total_existing_car_stock)

    # 4. load consumption data
    avg_km_per_ev = calculate_avg_km_by_car(year=year)
    avg_mwh_per_km = calculate_avg_mwh_per_km()


    # 5. calculate the consumption
    ev_consumption = calculate_electric_vehicle_consumption(data_in=number_of_evs, avg_km_per_ev=avg_km_per_ev, avg_mwh_per_km=avg_mwh_per_km)


    # 6. dissaggregate the total consumption into region_ids
    ev_consumption_by_region = regional_dissaggregation_ev_consumption(ev_consumption=ev_consumption)


    # 7. get only the private vehicles consumption
    cosumption_of_commercial_vehicles = ev_consumption_by_region * share_of_commercial_vehicles_regional_id
    cosumption_of_private_vehicles = ev_consumption_by_region - cosumption_of_commercial_vehicles


    return cosumption_of_private_vehicles


def future_2_electric_vehicle_consumption(year: int, szenario: str = "trend") -> pd.DataFrame:
    """
    Calculate the future consumption of electric vehicles based on the number of electric vehicles and the average km per ev and the average mwh per km.

    Predicted EV market penetration based on different szenarios from literature.

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
    if year < LAST_YEAR_EXISTING_DATA_KBA or year > 2045:
        raise ValueError("Year must be between {LAST_YEAR_EXISTING_DATA_KBA} and 2045, year is " + str(year))
    if szenario not in ["ambit", "trend", "regio"]:
        raise ValueError("Szenario must be in ['ambit', 'trend', 'regio']")
    
    
    # 1. calculate the new number of electric vehicles in the year
    # political target: 15mio electric vehicles (E-autos, nicht LKW) by 2030
    number_of_evs = s2_future_ev_stock(year=year, szenario=szenario)


    # 2. load data
    avg_km_per_ev = calculate_avg_km_by_car(year=year)
    avg_mwh_per_km = calculate_avg_mwh_per_km()
    share_of_commercial_vehicles_regional_id = share_of_commercial_vehicles_by_regional_id(year=year)


    # 3. calculate the consumption
    ev_consumption = calculate_electric_vehicle_consumption(data_in=number_of_evs, avg_km_per_ev=avg_km_per_ev, avg_mwh_per_km=avg_mwh_per_km)


    # 4. dissaggregate the total consumption into region_ids
    ev_consumption_by_region = regional_dissaggregation_ev_consumption(ev_consumption=ev_consumption)

    # 5. get only the private vehicles consumption
    cosumption_of_commercial_vehicles = ev_consumption_by_region * share_of_commercial_vehicles_regional_id
    cosumption_of_private_vehicles = ev_consumption_by_region - cosumption_of_commercial_vehicles


    return cosumption_of_private_vehicles


# main function for s1 and s2
def s1_2_electric_vehicle_consumption(year: int, szenario: str, s2_szenario: str) -> pd.DataFrame:
    """
    Calculate the future consumption of electric vehicles based on the number of electric vehicles and the average km per ev and the average mwh per km.

    Covers the following szenarios:
        - KBA_1: future data based on political target 15mio EVs by 2030
        - KBA_2: future data based on different szenarios from literature

    Args:
        year: int
            Year to load the data for
        szenario: str
            Szenario to load the data for [KBA_1, KBA_2]
        s2_szenario: str
            Szenario to load the data for KBA_2

    Returns:
        pd.DataFrame
            index: regional_id
            columns: power[mwh]
    """

    # 0. validate input
    if year < FIRST_YEAR_EXISTING_DATA_KBA or year > 2045:
        raise ValueError(f"Year must be between {FIRST_YEAR_EXISTING_DATA_KBA} and 2045 but is {year}")
    
    # 1. load data
    if year <= LAST_YEAR_EXISTING_DATA_KBA:
        ev_consumption_by_region = historical_electric_vehicle_consumption(year=year)
    elif year > LAST_YEAR_EXISTING_DATA_KBA:
        if szenario == "KBA_1":
            ev_consumption_by_region = future_1_electric_vehicle_consumption(year=year)
        elif szenario == "KBA_2":
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
# ! for KVB_1 & KVB_2 this returns the total consumption of cars ( home_charging + work_charging + public_charging)
# ! for UGR this only returns the consumption of home_charging
def electric_vehicle_consumption_by_regional_id(year: int, szenario: str, s2_szenario: str = None, force_preprocessing: bool = False) -> pd.DataFrame:
    """
    Loads the registered electric vehicles by regional id for the given year in the past or future

    Covers the following szenarios:
        - KBA: based on registered electric vehicles in Germany from "Kraftfahrt-Bundesamt"
            - historical data: registered electric vehicles by regional id
            - KBA_1: future data based on political target 15mio EVs by 2030
            - KBA_2: future data based on different szenarios from literature
        - UGR: based on fuel consumption in Germany from "Umweltökonomische Gesamtrechnung"
            - UGR: 
        - all szenarios predict 2045 only EVs

    Args:
        year: int
            Year to load the data for
        szenario: str
            Szenario to load the data for
        s2_szenario: str
            Szenario to load the data for
        force_preprocessing: bool
            If True, the data will be preprocessed even if the cache file exists

    Returns:
        pd.DataFrame: DataFrame with the registered electric vehicles by regional id for the given year
            - index: regional_id
            - columns: power[mwh]
    """

    # 0. validate input
    if szenario == "KBA_1" or szenario == "KBA_2":
        if year < FIRST_YEAR_EXISTING_DATA_KBA or year > 2045:
            raise ValueError(f"Year must be between {FIRST_YEAR_EXISTING_DATA_KBA} and 2045 but is {year}")        
    elif szenario == "UGR":
        if year < FIRST_YEAR_EXISTING_DATA_UGR or year > 2045:
            raise ValueError(f"Year must be between {FIRST_YEAR_EXISTING_DATA_UGR} and 2045 but is {year}")
        if s2_szenario is not None:
            raise ValueError("No s2_szenario for UGR - must be None ")
    else:
        raise ValueError("szenario must be in ['KBA_1', 'KBA_2', 'UGR']")
    if szenario == "KBA_2" and s2_szenario not in ["ambit", "trend", "regio", None]:
        raise ValueError(f"s2_szenario must be in ['ambit', 'trend', 'regio'] but is {s2_szenario}")


    # 0.1 check the cache
    cache_dir = load_config("base_config.yaml")['electric_vehicle_consumption_by_regional_id_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['electric_vehicle_consumption_by_regional_id_cache_file'].format(year=year, szenario=szenario, s2_szenario=s2_szenario))

    if os.path.exists(cache_file) and not force_preprocessing:
        logger.info(f"Load electric_vehicle_consumption_by_regional_id from cache for year: {year}, szenario: {szenario}, s2_szenario: {s2_szenario}")
        ev_consumption_by_region =  pd.read_csv(cache_file)
        ev_consumption_by_region.set_index("regional_id", inplace=True)
        return ev_consumption_by_region


    # 1. load the data
    # KBA Szenario
    if "KBA" in szenario:
        ev_consumption_by_region = s1_2_electric_vehicle_consumption(year=year, szenario=szenario, s2_szenario=s2_szenario)        
        
    # UGR Szenario
    elif "UGR" in szenario:
        ev_consumption_by_region = s3_electric_vehicle_consumption(year=year)


    # 2. set the index to regional_id
    if "regional_id" in ev_consumption_by_region.columns:
        ev_consumption_by_region.set_index("regional_id", inplace=True)


    # 2. save the data to the cache
    if ev_consumption_by_region.isna().any().any():
        raise ValueError("DataFrame contains NaN values")
    os.makedirs(cache_dir, exist_ok=True)
    logger.info(f"Save electric_vehicle_consumption_by_regional_id to cache for year: {year}, szenario: {szenario}, s2_szenario: {s2_szenario}")
    ev_consumption_by_region.to_csv(cache_file)    


    return ev_consumption_by_region





