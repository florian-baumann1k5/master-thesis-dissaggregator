import pandas as pd

from src.data_processing.heat import *
from src.pipeline.pipe_applications import *



def temporal_cts_elec_load_from_fuel_switch():
    """
    """

    sector = "cts"
    switch_to = "power"
    year = 2030
    state = "BW"

    df_gas_switch = sector_fuel_switch_fom_gas(sector=sector, switch_to=switch_to, year=year)


    df2 = disagg_temporal_cts_fuel_switch(df_gas_switch=df_gas_switch, state=state, year=year)



    return None


def sector_fuel_switch_fom_gas(sector: str, switch_to: str, year: int) -> pd.DataFrame:
    """
    Determines yearly gas demand per branch and regional id for heat applications
    that will be replaced by power or hydrogen in the future.

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


    df_gas_switch = pd.DataFrame(index=df_consumption.index,
                                 columns=df_consumption.columns,
                                 data=0)
    

    # 3. Ensure all index and column labels are strings to avoid alignment issues
    df_consumption.columns = pd.MultiIndex.from_tuples(
        [(str(b), str(a)) for b, a in df_consumption.columns],
        names=df_consumption.columns.names
    )
    fuel_switch_projected.index = fuel_switch_projected.index.map(str)
    fuel_switch_projected.columns = fuel_switch_projected.columns.map(str)

    
    # 5. multiply the fuel switch share with the consumption data
    fs_stacked = fuel_switch_projected.stack(dropna=True)
    multiplier_series = fs_stacked.reindex(df_consumption.columns, fill_value=0)
    df_gas_switch = df_consumption * multiplier_series


    # 6. Drop columns with all zeros
    # df_gas_switch = df_gas_switch.loc[:, (df_gas_switch != 0).any(axis=0)]
    all_zero_cols = df_gas_switch.columns[(df_gas_switch == 0).all(axis=0)]
    """
    if len(all_zero_cols) > 0:
        print("Dropped columns (all zero):")
        for col in all_zero_cols:
            print(col)
    """
    df_gas_switch = df_gas_switch.drop(columns=all_zero_cols)



    return df_gas_switch



def disagg_temporal_cts_fuel_switch(df_gas_switch: pd.DataFrame, state: str, year: int) -> pd.DataFrame:
    """
    """

    # 0. validate inputs




    # 1. create a multicolumn dataframe from the given dataframe and state:

    new_df = make_3level_timeseries(df_gas_switch=df_gas_switch, state=state, year=year)
    """ new_df is a dataframe with the following columns:

    columns:            ['regional_id', 'industry_sector', 'application']
    index:              [datetime]: year in 15 min timesteps
    values:             [float]: 0
    
    """

    print(new_df)
    # 2. 


    # get normalized timeseries for temperature dependent and temperature
    # independent gas demand in CTS
    heat_norm, gas_total, gas_tempinde_norm = create_heat_norm_cts(detailed=True, state=state, year=year)

    return None

