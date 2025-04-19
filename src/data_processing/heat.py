import pandas as pd

from src.data_access.local_reader import *

def get_fuel_switch_share(sector: str, switch_to: str) -> pd.DataFrame:

    """
    Read fuel switch shares by branch from input data for year 2045.

    Parameters
    ----------
    sector : str
        must be one of ['cts', 'industry']
    switch_to: str
        must be one of ['power', 'hydrogen']

    Returns
    -------
    pd.DataFrame
        - index: industry_sector
        - columns: applications
    """

    # 0. validate inputs
    if sector not in ['cts', 'industry']:
        raise ValueError(f"Invalid sector: {sector}")
    if switch_to not in ['power', 'hydrogen']:
        raise ValueError(f"Invalid switch_to: {switch_to}")


    # 1. load data
    df_fuel_switch = load_fuel_switch_share(sector=sector, switch_to=switch_to)
    # clean data (only keep rows with data)
    df_fuel_switch = df_fuel_switch.loc[lambda d: d["WZ"].apply(lambda x: isinstance(x, (int, np.integer)))]
    # rename Wz to industry sector and set as index
    df_fuel_switch = df_fuel_switch.rename(columns={"WZ": "industry_sector"})
    df_fuel_switch = df_fuel_switch.set_index("industry_sector")


    return df_fuel_switch



def projection_fuel_switch_share(df_fuel_switch: pd.DataFrame, target_year: int, base_year: int = 2019, final_year: int = 2045) -> pd.DataFrame:
    """
    Projects fuel switch share by branch to target year:
    Linearly project the fuel‑switch shares from base_year to final_year.

    - If target_year ≤ base_year: all shares → 0  
    - If base_year < target_year ≤ final_year: scale by fraction  
    - If target_year > final_year: leave as is  

    Args:
        df_fuel_switch : pd.DataFrame()
            Data which is projected.
        target_year: int
            Year for which the share should be projected.
        base_year: int
            Base year of the data.
        final_year: int
            Final year of the data.

    Returns:
        pd.DataFrame
            Projected fuel switch share by branch.
    """

    # 1. check if target year is before or after base year
    if target_year <= base_year:
        logger.info(f"Target year ({target_year}) ≤ base year ({base_year}). "
            "No projection; returning zeros.")
        # Set all values to zero
        df_fuel_switch = df_fuel_switch.mul(0)

    # 2. project to target year
    elif target_year <= final_year:
        # define yearly step from today to 2045
        df_scaling = df_fuel_switch.div(final_year-base_year)
        # project to target year
        df_fuel_switch_projected = df_scaling * (target_year - base_year)
        df_fuel_switch = df_fuel_switch_projected

    # 3. return df_fuel_switch
    return df_fuel_switch