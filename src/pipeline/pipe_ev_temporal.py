import pandas as pd


from src.configs.mappings import *
from src.configs.config_loader import load_config
from src.data_processing.electric_vehicles import *
from src.pipeline.pipe_ev_regional_consumption import *




def electric_vehicle_consumption_by_region_id_and_temporal_resolution(year: int, szenario: str, s2_szenario: str,) -> pd.DataFrame:
    """
    TODO: description
    """


    # 0. validate input
    if szenario == "KVB_1" or szenario == "KVB_2":
        if year < FIRST_YEAR_EXISTING_DATA_KVB or year > LAST_YEAR_EXISTING_DATA_KVB:
            raise ValueError(f"Year must be between {FIRST_YEAR_EXISTING_DATA_KVB} and {LAST_YEAR_EXISTING_DATA_KVB} but is {year}")        
    elif szenario == "UGR":
        if year < FIRST_YEAR_EXISTING_DATA_UGR or year > LAST_YEAR_EXISTING_DATA_UGR:
            raise ValueError(f"Year must be between {FIRST_YEAR_EXISTING_DATA_UGR} and {LAST_YEAR_EXISTING_DATA_UGR} but is {year}")
    else:
        raise ValueError("szenario must be in ['KVB_1', 'KVB_2', 'UGR']")
    if szenario == "KVB_2" and s2_szenario not in ["ambit", "trend", "regio"]:
        raise ValueError("s2_szenario must be in ['ambit', 'trend', 'regio']")

    
    # 1. load data
    ev_consumption_by_regional_id = electric_vehicle_consumption_by_regional_id(year=year, szenario=szenario, s2_szenario=s2_szenario)

    # 2. disaggregate the data by temporal resolution
    #ev_consumption_by_regional_id_and_temporal_resolution = disaggregate_temporal_ev_consumption(ev_consumption_by_regional_id=ev_consumption_by_regional_id)

    return None



