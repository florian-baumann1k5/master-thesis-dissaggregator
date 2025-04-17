import pandas as pd
from src.data_processing.temporal import *






def disaggregate_temporal(energy_carrier: str, sector: str, year: int) -> pd.DataFrame:
    """
    Disaggregate the temporal data for a given energy carrier and sector.
    """


    # 1. Get the consumption data
    consumption_data = disagg_applications_efficiency_factor(sector=sector, energy_carrier=energy_carrier, year=year)



    if sector == "industry":
        # aggregate the application dissaggregation
        consumption_data = consumption_data.groupby(level=0, axis=1).sum()
        # disaggregate the consumption data
        df = disaggregate_temporal_industry(consumption_data=consumption_data, year=year, low=0.5)


    elif sector == "cts":
        # get the consumption data
        if energy_carrier == "gas":
            
            
            
            #TODO: 
            consumption_data = consumption_data.groupby(level=0, axis=1).sum()

            df = disagg_temporal_gas_CTS(gas_consumption=consumption_data, year=year)





    return None








