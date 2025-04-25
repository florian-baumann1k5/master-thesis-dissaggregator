import pandas as pd
import numpy as np

from src.data_access.local_reader import *
from src.configs.mappings import *
from src.data_processing.temporal import *
from src.data_processing.temperature import *



def get_fuel_switch_share(sector: str, switch_to: str) -> pd.DataFrame:

    """
    Read fuel switch shares by branch from input data for year 2045.
    Gas switch to power or hydrogen in the year 2045.

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
    df_fuel_switch = df_fuel_switch.loc[lambda d: d['industry_sector'].apply(lambda x: isinstance(x, (int, np.integer)))]
    # rename Wz to industry sector and set as index
    df_fuel_switch = df_fuel_switch.set_index('industry_sector')


    return df_fuel_switch



def projection_fuel_switch_share(df_fuel_switch: pd.DataFrame, target_year: int, base_year: int = 2019, final_year: int = 2045) -> pd.DataFrame:
    """
    Projects fuel switch share by branch to target year:
    Linearly project the fuel‑switch shares from base_year to final_year.

    Deafualt is 2045 since germany want to use no fossil gas by 2045.
    In this function we assume the reduction of gas demand is linear.

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
        df_scaling = df_fuel_switch.div(final_year - base_year)
        # project to target year
        df_fuel_switch_projected = df_scaling * (target_year - base_year)
        df_fuel_switch = df_fuel_switch_projected

    # 3. return df_fuel_switch
    return df_fuel_switch



def make_3level_timeseries(df_gas_switch: pd.DataFrame, state: str, year: int ) -> pd.DataFrame:
    """
    Returns a DataFrame indexed by 15‑min timestamps through 'year',
    with columns = MultiIndex [regional_id, industry_sector, application].
    Only regional_ids belonging to 'state' are included.
    All cells are initialized to 0.0.

    Parameters
    ----------
    df_gas_switch : pd.DataFrame
        Index   -> regional_id (strings or ints)
        Columns -> 2‑level MultiIndex (industry_sector, application)

    state : str
        Two‑letter state abbreviation, e.g. 'SH', 'NW', ...

    year : int
        Calendar year for the time index.

    Returns
    -------
    pd.DataFrame
        index   -> 15‑min DateTimeIndex for the whole year
        columns -> MultiIndex(levels=[regional_id, industry_sector, application])
    """
    # 1) Map regional_id → state
    state_map = federal_state_dict()            # e.g. {1:'SH', 2:'HH', …}
    valid_rids = []
    for rid in df_gas_switch.index:
        # ensure string
        rid_str = str(rid)
        # drop last 3 chars, parse to int
        key = int(rid_str[:-3])
        if state_map.get(key) == state:
            valid_rids.append(rid)

    if not valid_rids:
        raise ValueError(f"No regional_id for state '{state}' found in df_gas_switch.index")

    # 2) pull unique sectors & applications
    sectors = df_gas_switch.columns.get_level_values(0).unique()
    apps    = df_gas_switch.columns.get_level_values(1).unique()

    # 3) build 3‑level MultiIndex for columns
    cols = pd.MultiIndex.from_product(
        [valid_rids, sectors, apps],
        names=["regional_id", "industry_sector", "application"],
    )

    # 4) build 15‑min time index for the full year
    time_index = pd.date_range(
        start=f"{year}-01-01 00:00",
        end  =f"{year}-12-31 23:45",
        freq ="15T",
        tz   =None
    )

    # 5) create empty DataFrame
    new_df = pd.DataFrame(index=time_index, columns=cols, data=0.0)
    return new_df


def create_heat_norm_cts(state: str, year: int) -> pd.DataFrame:
    """
    Creates normalised heat demand timeseries for CTS per regional_id, and branch

    Args:
        detailed : bool, default False
            If True heat demand per branch and disctrict is calculated.
            Otherwise just the heat demand per district.
        state : str, default None
            Specifies state. Only needed if detailed=True. Must by one of the
            entries of bl_dict().values(),
            ['SH', 'HH', 'NI', 'HB', 'NW', 'HE', 'RP', 'BW', 'BY', 'SL', 'BE',
            'BB', 'MV', 'SN', 'ST', 'TH']

    Returns:
        heat_norm : pd.DataFrame
            normalised heat demand
            index = datetimeindex: hourly
            columns = [regional_id, industry_sector]
        gas_total : pd.DataFrame
            total gas consumption
            index = datetimeindex: hourly
            columns = [regional_id, industry_sector]
        gas_tempinde_norm : pd.DataFrame
            gas consumption for temoeratureindependent applications
            (hot water, process heat, mechanical energy for CTS)
            index = datetimeindex: hourly
            columns = [regional_id, industry_sector]
    """


    #1. get the consumption data
    consumption_data = disagg_applications_efficiency_factor(sector="cts", energy_carrier="gas", year=year)
    consumption_data = consumption_data.T.groupby(level=0).sum().T


    # 2. get total consumption of all applications by regional_id
    state_list = [state]
    gas_total = disagg_temporal_gas_CTS(consumption_data=consumption_data, state_list=state_list, year=year)


    # 3. get the consumption of ['hot_water', 'mechanical_energy', 'process_heat'] by regional_id
    gas_tempinde = (disagg_temporal_gas_CTS_water_by_state(state=state, year=year))
    

    # 4. create space heating timeseries: difference between total heat demand and water heating demand
    heat_norm = (gas_total - gas_tempinde).clip(lower=0)


    # 5. get the temperature allocation
    temp_allo = allocation_temperature_by_hour(year=year)


    # 6. clip heat demand above heating threshold
    heat_total = heat_norm.droplevel(level=1, axis=1)
    mask = temp_allo[temp_allo > 15].isnull()
    mask.index = pd.to_datetime(mask.index)
    mask.columns = mask.columns.astype(int)
    heat_masked = heat_total[mask]
    df = heat_masked.fillna(0)


    df.columns = heat_norm.columns
    heat_norm = df.copy()


    # 7. normalise (sum per industry sector = 1.0)
    heat_norm = heat_norm.divide(heat_norm.sum(axis=0), axis=1)
    heat_norm = heat_norm.fillna(0.0)

    gas_tempinde_norm = gas_tempinde.divide(gas_tempinde.sum(axis=0), axis=1)
    gas_tempinde_norm = gas_tempinde_norm.fillna(0.0)


    return heat_norm, gas_total, gas_tempinde_norm

