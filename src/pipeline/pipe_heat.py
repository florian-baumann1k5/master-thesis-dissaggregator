import pandas as pd

from src.data_processing.heat import *
from src.pipeline.pipe_applications import *


def sector_fuel_switch_fom_gas(sector: str, switch_to: str, year: int) -> pd.DataFrame:
    """
    Determines yearly gas demand per branch and regional id for heat applications
    that will be replaced by a different fuel in the future.
    Fuel is specified by parameter 'switch_to'.

    Parameters
    ----------
    sector : str
        must be one of ['cts', 'industry']
    switch_to: str
        must be one of ['power', 'hydrogen']

    Returns
    -------
    pd.DataFrame

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

    # Stack fuel_switch_projected to create a Series with a MultiIndex (branch, app)
    # This aligns the scalar multiplication factors with the column structure.
    # dropna=False can be included if fuel_switch_projected might contain NaNs that should be preserved,
    # otherwise default dropna=True is usually fine.
    fs_stacked = fuel_switch_projected.stack(dropna=True)

    # Reindex the stacked Series to match the columns of df_consumption.
    # This ensures that we only consider (branch, app) pairs present in df_consumption
    # and assigns a multiplier of 0 to pairs present in df_consumption but not
    # in fuel_switch_projected, matching the implicit logic of the original loop
    # and the initialization of df_gas_switch with zeros.
    multiplier_series = fs_stacked.reindex(df_consumption.columns, fill_value=0)

    # Perform the element-wise multiplication using vectorized Pandas operations.
    # This calculates the final df_gas_switch directly, eliminating the need
    # for pre-initialization and loops.
    df_gas_switch = df_consumption * multiplier_series

    # Drop columns with all zeros
    df_gas_switch = df_gas_switch.loc[:, (df_gas_switch != 0).any(axis=0)]



    # TODO: this only works for future consumption data
    return None