import pandas as pd

from src.configs.mappings import *
from src.configs.config_loader import load_config
from src.data_processing.electric_vehicles import *
from src.pipeline.pipe_ev_regional_consumption import *
from src.utils.utils import *


FIRST_YEAR_EXISTING_DATA_KVB = load_config()["first_year_existing_registration_data"]
LAST_YEAR_EXISTING_DATA_KVB = load_config()["last_year_existing_registration_data"]
FIRST_YEAR_EXISTING_DATA_UGR = load_config()["first_year_existing_fuel_consumption_ugr"]
LAST_YEAR_EXISTING_DATA_UGR = load_config()["last_year_existing_fuel_consumption_ugr"]


def electric_vehicle_consumption_by_region_id_and_temporal_resolution(year: int, szenario: str, s2_szenario: str, state: str) -> pd.DataFrame:
    """
    TODO: description
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
    if state not in federal_state_dict().values():
        raise ValueError(f"state must be in {federal_state_dict().values()}")

    
    # 1. load data
    ev_consumption_by_regional_id = electric_vehicle_consumption_by_regional_id(year=year, szenario=szenario, s2_szenario=s2_szenario)


    #2. build the mask
    mask = create_weekday_workday_holiday_mask(state=state, year=year)

    # 3. load the charging profiles
    ev_charging_profile = load_ev_charging_profile(type="power", day_type="weekday")

    # 4. disaggregate the data by temporal resolution
    ev_consumption_by_regional_id_and_temporal_resolution = disaggregate_temporal_ev_consumption(ev_consumption_by_regional_id=ev_consumption_by_regional_id)

    return None






