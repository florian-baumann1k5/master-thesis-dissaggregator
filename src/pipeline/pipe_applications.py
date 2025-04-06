import pandas as pd

from src.data_processing.effects import *
from src.pipeline.pipe_consumption import *
from src.data_processing.application import *
from src.data_access.local_reader import *
"""
Dissaggregating the consumption data (per industry sector and regional_id) based on their applications.
"""

def disagg_applications_efficiency_factor(sector: str, energy_carrier: str, year: int) -> pd.DataFrame: #TODO
    """
    
    """

    # 0. validate the input
    if sector not in ['cts', 'industry']:
        raise ValueError("`sector` must be 'cts' or 'industry'")
    if energy_carrier not in ['power', 'gas', 'petrol']:
        raise ValueError("`energy_carrier` must be 'power', 'gas' or 'petrol'")


    # 1. get consumption data dissaggregated by industry sector and regional_id
    # TODO: consumption_data = get_consumption_data(year)
    consumption_data = get_employees_per_industry_sector_and_regional_ids(year=year)

    # 2. filter for relevant sector:
    sectors_industry_sectors = dict_cts_or_industry_per_industry_sector()[sector]
    consumption_data = consumption_data.loc[consumption_data.index.intersection(sectors_industry_sectors)]

    # 3. dissaggregate for applications - cosnumption data is already fiilterd to contain only relevant industry_sectors(cts/industry)
    consumption_data_dissaggregated = dissaggregate_for_applications(consumption_data=consumption_data,year=year, sector=sector, energy_carrier=energy_carrier)



    # apply efficiency effect
    efficiency_rate = apply_efficiency_factor(consumption_data=consumption_data_dissaggregated, sector=sector, energy_carrier=energy_carrier, year=year)





    return year



