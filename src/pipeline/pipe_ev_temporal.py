import pandas as pd

from src.configs.mappings import *
from src.configs.config_loader import load_config
from src.data_processing.electric_vehicles import *
from src.pipeline.pipe_ev_regional_consumption import *
from src.utils.utils import *


FIRST_YEAR_EXISTING_DATA_KBA = load_config()["first_year_existing_registration_data"]
LAST_YEAR_EXISTING_DATA_KBA = load_config()["last_year_existing_registration_data"]
FIRST_YEAR_EXISTING_DATA_UGR = load_config()["first_year_existing_fuel_consumption_ugr"]
LAST_YEAR_EXISTING_DATA_UGR = load_config()["last_year_existing_fuel_consumption_ugr"]




def electric_vehicle_consumption_by_region_id_and_temporal_resolution(year: int, szenario: str, s2_szenario: str = None, force_preprocessing: bool = False) -> pd.DataFrame:
    """
    This function disaggregates the ev consumption by regional id to a temporal resolution (10min steps).
    

    Args:
        year (int): The year of the data.
        szenario (str): The scenario of the data for the ev consumption by regional id.
        s2_szenario (str, optional): The s2 scenario of the data for the ev consumption by regional id. Defaults to None.

    Returns:
        pd.DataFrame: The ev consumption by regional id and temporal resolution.
            - index: daytime
            - column[0]: regional_ids
            - column[1]: charging_consumption [home_charging, work_charging, public_charging]
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
        raise ValueError("s2_szenario must be in ['ambit', 'trend', 'regio', None]")
    

    # 0.1 load from cache if available
    cache_dir = load_config("base_config.yaml")['electric_vehicle_consumption_by_regional_id_temporal_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['electric_vehicle_consumption_by_regional_id_temporal_cache_file'].format(year=year, szenario=szenario, s2_szenario=s2_szenario))

    if os.path.exists(cache_file) and not force_preprocessing:
        logger.info(f"Load electric_vehicle_consumption_by_regional_id from cache for year: {year}, szenario: {szenario}, s2_szenario: {s2_szenario}")
        ev_consumption_by_region =  pd.read_csv(cache_file)
        ev_consumption_by_region.set_index("regional_id", inplace=True)
        return ev_consumption_by_region

    
    # 1. load data
    ev_consumption_by_regional_id = electric_vehicle_consumption_by_regional_id(year=year, szenario=szenario, s2_szenario=s2_szenario)



    # 2. disaggregate the data by temporal resolution
    ev_consumption_by_regional_id_and_temporal_resolution = pd.DataFrame()
    state_counter = 1

    # 2.1. iterate over all states ( to also include state-holidays)
    for state in federal_state_dict().values():
        logger.info(f"Disaggregating ev consumption by state {state_counter}/{len(federal_state_dict().values())}: {state}")
        state_counter += 1


        # 2.2. generate yearly charging profile (state based to include state-holidays)
        yearly_charging_profile = get_normalized_yearly_ev_charging_profile(year=year, state=state)


        # 2.3. disaggregate the data by temporal resolution
        ev_consumption_by_state = disaggregate_temporal_ev_consumption_for_state(ev_consumption_by_regional_id=ev_consumption_by_regional_id, state=state, year=year, yearly_charging_profile=yearly_charging_profile)


        # 2.4. append the result
        ev_consumption_by_regional_id_and_temporal_resolution = pd.concat([ev_consumption_by_regional_id_and_temporal_resolution, ev_consumption_by_state], axis=1)



    # 6. validate the result
    if ev_consumption_by_regional_id_and_temporal_resolution.isnull().any().any():
        raise ValueError("There are still NaNs in the result")
    if not np.isclose(ev_consumption_by_regional_id_and_temporal_resolution.sum().sum(), ev_consumption_by_regional_id.sum().sum()):
        raise ValueError("The sum of the ev consumption by regional id temporal is not equal to the sum of the ev consumption by regional id!")
    

    # 7. save to cache
    os.makedirs(cache_dir, exist_ok=True)
    logger.info(f"Save ev_consumption_by_regional_id_and_temporal_resolution to cache for year: {year}, szenario: {szenario}, s2_szenario: {s2_szenario}")
    ev_consumption_by_regional_id_and_temporal_resolution.to_csv(cache_file)   


    return ev_consumption_by_regional_id_and_temporal_resolution






