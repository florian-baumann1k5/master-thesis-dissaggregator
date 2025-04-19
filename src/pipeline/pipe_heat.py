import pandas as pd

from src.data_processing.heat import *
from src.pipeline.pipe_applications import *


def sector_fuel_switch_fom_gas(sector: str, switch_to: str, year: int) -> pd.DataFrame:
    """
    """

    # 0. validate inputs
    if sector not in ['cts', 'industry']:
        raise ValueError(f"Invalid sector: {sector}")
    if switch_to not in ['power', 'hydrogen']:
        raise ValueError(f"Invalid switch_to: {switch_to}")


    # 1. load consumption data by application and wz and region
    df_consumption = disagg_applications_efficiency_factor(year=year, energy_carrier="gas", sector=sector)


    # 1. load data
    df_fuel_switch = get_fuel_switch_share(sector=sector, switch_to=switch_to)


    # 2. project fuel switch share to year
    fuel_switch_projected = projection_fuel_switch_share(df_fuel_switch = df_fuel_switch,
                                                         target_year=year)














    return None