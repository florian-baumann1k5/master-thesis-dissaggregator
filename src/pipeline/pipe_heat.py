import pandas as pd

from src.data_processing.heat import *
from src.pipeline.pipe_applications import *



def temporal_cts_elec_load_from_fuel_switch():
    """
    """

    sector = "cts"
    switch_to = "power"
    year = 2030
    state = "SH"

    df_gas_switch = sector_fuel_switch_fom_gas(sector=sector, switch_to=switch_to, year=year)


    df2 = disagg_temporal_cts_fuel_switch(df_gas_switch=df_gas_switch, state=state, year=year)



    return None


def sector_fuel_switch_fom_gas(sector: str, switch_to: str, year: int) -> pd.DataFrame:
    """
    Determines yearly gas demand per branch and regional id for heat applications
    that will be replaced by power or hydrogen in the future.

    Args:
        sector : str
            must be one of ['cts', 'industry']
        switch_to: str
            must be one of ['power', 'hydrogen']

    Returns:
        pd.DataFrame:
            index: regional_id
            columns: [industry_sector, application]
            values: gas demand that needs to be replaced by power or hydrogen

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
    Temporally disaggregates CTS gas demand, which will be switched to
    electricity or hydrogen, by state.

    Args:
        df_gas_switch : pd.DataFrame
            Gas demand by branch, application and NUTS-3 region which will be
            replaced.
        state : str
            Specifies state. Must by one of the entries of bl_dict().values(),
            ['SH', 'HH', 'NI', 'HB', 'NW', 'HE', 'RP', 'BW', 'BY', 'SL', 'BE',
                'BB', 'MV', 'SN', 'ST', 'TH']
        year : int
            Year of the data
    
    Returns:
        pd.DataFrame() : timestamp as index, multicolumns with nuts-3, branch and
            applications. temperature dependent and independent profiles from gas
            SLP for temporal disaggregation of df_gas_switch.

        
    """

    # 0. validate inputs
    if state not in federal_state_dict().values():
        raise ValueError(f"Invalid state: {state}")
    if year not in range(2019, 2051):
        raise ValueError(f"Invalid year: {year}")



    # 1. create a multicolumn dataframe from the given dataframe and state:
    new_df = make_3level_timeseries(df_gas_switch=df_gas_switch, state=state, year=year)
    """ new_df is a dataframe with the following columns:

    columns:            ['regional_id', 'industry_sector', 'application']
    index:              [datetime]: year in 15 min timesteps
    values:             [float]: 0
    
    """


    # 2. get normalized timeseries for temperature dependent and temperature
    # independent gas demand in CTS - hourly
    heat_norm, gas_total, gas_tempinde_norm = create_heat_norm_cts(state=state, year=year)



    # 3. heat_norm: transform it into a 15-minute resolution using interpolation and normalize
    """ interpolation:
    00:00 → 10.0
    00:15 → NaN
    00:30 → NaN
    00:45 → NaN
    01:00 → 11.0
    ->
    00:00 → 10.0
    00:15 → 10.25
    00:30 → 10.5
    00:45 → 10.75
    01:00 → 11.0
    """
    heat_norm = (heat_norm
        .resample('15min')
        .asfreq()
        .interpolate(method='linear', limit_direction='forward', axis=0)
    )
    extension = pd.DataFrame(
        index=pd.date_range(
            heat_norm.index[-1:].values[0],
            periods=4,
            freq='15min'
        )[-3:],
        columns=heat_norm.columns
    )
    heat_norm = pd.concat([heat_norm, extension]).fillna(method='ffill')

    heat_norm = heat_norm.divide(heat_norm.sum(), axis=1)


    # 4.gas_tempinde_norm: transform it into a 15-minute resolution using interpolation and normalize
    gas_tempinde_norm = (gas_tempinde_norm
        .resample('15min').asfreq()
        .interpolate(method='linear', limit_direction='forward', axis=0))
    extension = pd.DataFrame(
        index=pd.date_range(gas_tempinde_norm.index[-1:]
            .values[0],
            periods=4,
            freq='15min'
        )[-3:],
        columns=gas_tempinde_norm.columns)
    gas_tempinde_norm = (pd.concat([gas_tempinde_norm, extension]).fillna(method='ffill'))

    gas_tempinde_norm = gas_tempinde_norm.divide(gas_tempinde_norm.sum(), axis=1)



    # 5. create temp disaggregated gas demands per nuts-3, branch and app
    all_regional_ids = new_df.columns.get_level_values(0).unique()
    for regional_id in all_regional_ids:

        df_switch_region = df_gas_switch.loc[regional_id]
        for industry_sector in df_switch_region.index.get_level_values(0).unique():
            
            df_switch_branch = df_switch_region.loc[industry_sector]
            for app in df_switch_branch.index:

                if app == 'space_heating':
                    new_df[regional_id, industry_sector, app] = ((df_gas_switch.loc[regional_id][industry_sector, app]) * (heat_norm[regional_id, int(industry_sector)]))
                else:
                    new_df[regional_id, industry_sector, app] = ((df_gas_switch.loc[regional_id][industry_sector, app]) * (gas_tempinde_norm[regional_id, int(industry_sector)]))
        

    # 6. drop all columns that have only nan values
    new_df.dropna(axis=1, how='all', inplace=True)


    return new_df

