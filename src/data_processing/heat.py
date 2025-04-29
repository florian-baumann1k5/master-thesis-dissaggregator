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


    # 1. get the consumption data
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
    # DISS Formel 4.15
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


def calculate_tatal_demand_cts(df_temp_gas_switch: pd.DataFrame, p_ground: float, p_air: float, p_water: float) -> pd.DataFrame:
    """
    Calculates the total demand for each application in the DataFrame.

    Args:
        df_temp_gas_switch: pd.DataFrame
        p_ground: float
        p_air: float
        p_water: float

    Returns:
        pd.DataFrame
        index: datetimeindex: hourly
        columns: MultiIndex(levels=[regional_id, industry_sector, application])
    """

    # 0. validate inputs
    if p_ground + p_air + p_water != 1:
        raise ValueError("sum of percentage of ground/air/water heat pumps must be 1")
    


    col = pd.IndexSlice


    ## 1. Application: space_heating
    # 1.1 get efficiency level by application
    df_heating_switch = (df_temp_gas_switch.loc[:, col[:, :, ['space_heating']]] * get_efficiency_level_by_application('space_heating'))

    # 1.2 get the COP timeseries for indoor heating --> T=40°C
    air_floor_cop, ground_floor_cop, water_floor_cop = cop_ts(sink_t=40, source='ambient', year=year)
    assert (air_floor_cop.index.year.unique() == df_heating_switch.index.year.unique()), ("The year of COP ts does not match the year of the heat demand ts")

    # 1.3 calculate the total indoor heating demand
    df_temp_indoor_heating = (p_ground * (df_heating_switch.div(ground_floor_cop, level=0).fillna(method='ffill'))
                              + p_air * (df_heating_switch.div(air_floor_cop, level=0).fillna(method='ffill'))
                              + p_water * (df_heating_switch.div(water_floor_cop, level=0).fillna(method='ffill')))
    



    ## 2. Application: hot_water
    # 2.1 get efficiency level by application
    df_heating_switch = (df_temp_gas_switch.loc[:, col[:, :, ['process_heat']]] * get_efficiency_level_by_application('process_heat'))

    # 2.2 get the COP timeseries for process heating --> T=70°C
    air_floor_cop, ground_floor_cop, water_floor_cop = cop_ts(sink_t=70, source='ambient', year=year)

    # 2.3 calculate the total process heating demand
    df_temp_process_heat = (p_ground * (df_heating_switch.div(ground_floor_cop, level=0) .fillna(method='ffill'))
                            + p_air * (df_heating_switch.div(air_floor_cop, level=0).fillna(method='ffill'))
                            + p_water * (df_heating_switch.div(water_floor_cop, level=0).fillna(method='ffill')))
    



    ## 3. Application: hot_water
    # 3.1 get efficiency level by application
    df_heating_switch = (df_temp_gas_switch.loc[:, col[:, :, ['hot_water']]] * get_efficiency_level_by_application('hot_water'))

    # 3.2 get the COP timeseries for warm water  --> T=55°C
    air_floor_cop, ground_floor_cop, water_floor_cop = cop_ts(sink_t=55, source='ambient', year=year)

    # 3.3 calculate the total warm water demand
    df_temp_warm_water = (p_ground * (df_heating_switch.div(ground_floor_cop, level=0).fillna(method='ffill'))
                          + p_air * (df_heating_switch.div(air_floor_cop, level=0).fillna(method='ffill'))
                          + p_water * (df_heating_switch.div(water_floor_cop, level=0).fillna(method='ffill')))




    ## 4. Application: mechanical_energy
    df_mechanical_switch = ((df_temp_gas_switch
                                .loc[:, col[:, :, ['mechanical_energy']]]) * (get_efficiency_level_by_application('mechanical_energy') / 0.9))  
    # HACK! 0.9 = electric motor efficiency




    # 5. add all dataframes together for electric demand per regional_id, industry_sector and application
    df_temp_elec_from_gas_switch = pd.DataFrame(index=df_temp_gas_switch.index,
                                                columns=(df_temp_gas_switch.columns),
                                                data=0)
    df_temp_elec_from_gas_switch = (df_temp_elec_from_gas_switch
                                    .add(df_temp_indoor_heating, fill_value=0)
                                    .add(df_temp_process_heat, fill_value=0)
                                    .add(df_temp_warm_water, fill_value=0)
                                    .add(df_mechanical_switch, fill_value=0))
    

    return df_temp_elec_from_gas_switch
    

def create_heat_norm_industry(state: str, year: int, slp: str = 'KO') -> pd.DataFrame:
    """
    Creates normalised heat demand timeseries for industry per regional_id, and branch
    """


    #1. get the consumption data
    consumption_data = disagg_applications_efficiency_factor(sector="industry", energy_carrier="gas", year=year)
    consumption_data.index = consumption_data.index.map(int)


    # 2. merge the process heat
    process_columns = [
        'process_heat_below_100C',
        'process_heat_100_to_200C',
        'process_heat_200_to_500C',
        'process_heat_above_500C'
    ]

    mask = consumption_data.columns.get_level_values(1).isin(process_columns)
    process_heat_sum = (consumption_data.loc[:, mask].groupby(level=0, axis=1).sum())

    process_heat_sum.columns = pd.MultiIndex.from_product(
        [process_heat_sum.columns, ['process_heat']],
        names=consumption_data.columns.names
    )
    consumption_data = consumption_data.drop(columns=consumption_data.columns[mask])
    consumption_data = pd.concat([consumption_data, process_heat_sum], axis=1).sort_index(axis=1)



    #2. 
    col = pd.IndexSlice
    gv_lk_total = consumption_data.copy()
    gv_lk_tempinde = consumption_data.loc[:, col[:, ['hot_water', 'mechanical_energy', 'process_heat']]]



    # 3. get the temperature allocation
    temp_allo = allocation_temperature_by_day(year=year)
    # clip at 15 for hot water, below 15°C the water heating demand is assumed
    # to be constant
    temperatur_df_clip = temp_allo.clip(15)


    # 4. get hours of the year
    hours_of_year = get_hours_of_year(year)


    # 5. 
    regional_ids = get_regional_ids_by_state(state)
    

    # 6. new DataFrames for results
    gas_total = pd.DataFrame(columns=regional_ids, index=pd.date_range((str(year) + '-01-01'), periods=hours_of_year, freq='h'), dtype='float')
    gas_temp_inde = pd.DataFrame(columns=regional_ids, index=pd.date_range((str(year) + '-01-01'), periods=hours_of_year, freq='h'), dtype='float')
    

    # 7. get weekday-factors per day
    F_wd = (gas_slp_weekday_params(state, year=year).set_index('Date')['FW_'+str(slp)]).to_frame()
    # get h-value per day
    h_slp = h_value(slp, regional_ids, temp_allo)
    # get h-value for hot water per day
    h_slp_water = h_value_water(slp, regional_ids,  temperatur_df_clip)


    # 8. multiply h_values and week day values per day
    tw = pd.DataFrame(np.multiply(h_slp.values, F_wd.values), index=h_slp.index, columns=h_slp.columns.astype(int))
    tw_water = pd.DataFrame(np.multiply(h_slp_water.values, F_wd.values), index=h_slp_water.index, columns=h_slp_water.columns.astype(int))


    # 9. # normalize
    tw_norm = tw/tw.sum()
    tw_water_norm = tw_water/tw_water.sum()


    #10. set DatetimeIndex and multiply with gas demand per region
    tw_norm.index = pd.DatetimeIndex(tw_norm.index)
    tw_water_norm.index = pd.DatetimeIndex(tw_water_norm.index)
    
    ts_total = tw_norm.multiply(gv_lk_total.sum(axis=1).loc[regional_ids])
    ts_water = tw_water_norm.multiply(gv_lk_tempinde.sum(axis=1).loc[regional_ids])



    # 11. extend by one day because when resampling to hours later, this day is lost otherwise
    last_day = ts_total.copy()[-1:]
    last_day.index = last_day.index + timedelta(1)
    ts_total = pd.concat([ts_total, last_day]).resample('h').ffill()[:-1]
    # extend tw_water by one day and resample
    ts_water = pd.concat([ts_water, last_day]).resample('h').ffill()[:-1]


    # 12. get temperature dataframe for hourly disaggregation
    t_allo_df = temp_allo[[i for i in regional_ids]]
    for col in t_allo_df.columns:
        t_allo_df[col].values[t_allo_df[col].values < -15] = -15
        t_allo_df[col].values[(t_allo_df[col].values > -15)
                              & (t_allo_df[col].values < -10)] = -10
        t_allo_df[col].values[(t_allo_df[col].values > -10)
                              & (t_allo_df[col].values < -5)] = -5
        t_allo_df[col].values[(t_allo_df[col].values > -5)
                              & (t_allo_df[col].values < 0)] = 0
        t_allo_df[col].values[(t_allo_df[col].values > 0)
                              & (t_allo_df[col].values < 5)] = 5
        t_allo_df[col].values[(t_allo_df[col].values > 5)
                              & (t_allo_df[col].values < 10)] = 10
        t_allo_df[col].values[(t_allo_df[col].values > 10)
                              & (t_allo_df[col].values < 15)] = 15
        t_allo_df[col].values[(t_allo_df[col].values > 15)
                              & (t_allo_df[col].values < 20)] = 20
        t_allo_df[col].values[(t_allo_df[col].values > 20)
                              & (t_allo_df[col].values < 25)] = 25
        t_allo_df[col].values[(t_allo_df[col].values > 25)] = 100
        t_allo_df = t_allo_df.astype('int32')
    # for tempindependent consumption of ts_water t_allo_df = 100
    t_allo_water_df = t_allo_df.copy()
    t_allo_water_df.values[:] = 100 


    # 13. rewrite calendar for better data handling later
    calender_df = (gas_slp_weekday_params(state, year=year)[['Date', 'MO', 'DI', 'MI', 'DO', 'FR', 'SA', 'SO']])
    # add temperature data to calendar
    temp_calender_df = (pd.concat([calender_df.reset_index(), t_allo_df.reset_index()], axis=1))
    temp_calender_water_df = (pd.concat([calender_df.reset_index(), t_allo_water_df.reset_index()], axis=1))
    # add weekdays to calendar
    temp_calender_df['Tagestyp'] = 'MO'
    temp_calender_water_df['Tagestyp'] = 'MO'
    for typ in ['DI', 'MI', 'DO', 'FR', 'SA', 'SO']:
        (temp_calender_df.loc[temp_calender_df[typ], 'Tagestyp']) = typ
        (temp_calender_water_df.loc[temp_calender_water_df[typ], 'Tagestyp']) = typ


    # 15. apply gas load profile to total and temperature independent demand
    def calculate1(temp_cal, regional_id, final_df):
        logger.info(f"Calculating heat demand for {regional_id}")

        temp_cal = temp_cal[['Date', 'Tagestyp', regional_id]].set_index("Date")
        last_hour = temp_cal.copy()[-1:]
        last_hour.index = last_hour.index + timedelta(1)
        temp_cal = pd.concat([temp_cal, last_hour]).resample('h').ffill()[:-1]
        temp_cal['Stunde'] = pd.DatetimeIndex(temp_cal.index).time
        temp_cal = temp_cal.set_index(["Tagestyp", regional_id, 'Stunde'])

        slp_profil = load_gas_load_profile(slp)
        slp_profil = pd.DataFrame(slp_profil.set_index(['Tagestyp', 'Temperatur\nin °C\nkleiner']))
        slp_profil.columns = pd.to_datetime(slp_profil.columns, format='%H:%M:%S')
        slp_profil.columns = pd.DatetimeIndex(slp_profil.columns).time
        slp_profil = slp_profil.stack()

        temp_cal['Prozent'] = [slp_profil[x] for x in temp_cal.index]
        final_df[int(regional_id)] = (ts_total[regional_id].values * temp_cal['Prozent'].values/100)
        return final_df


    # 14. iterate over all regions
    for regional_id in regional_ids:
        gas_total = calculate1(temp_calender_df, regional_id, gas_total)
        gas_temp_inde = calculate1(temp_calender_water_df, regional_id, gas_temp_inde)


    # 15. create space heating timeseries: difference between total heat demand
    # and water heating demand
    heat_norm = (gas_total - gas_temp_inde).clip(lower=0)


    # 16. clip heat demand above heating threshold
    # DISS Formel 4.15
    temp_allo_all_regional_ids = allocation_temperature_by_hour(year=year)
    temp_allo_all_regional_ids.columns = temp_allo_all_regional_ids.columns.map(int)
    temp_allo_all_regional_ids.index = pd.to_datetime(temp_allo_all_regional_ids.index)
    temp_allo_all_regional_ids = temp_allo_all_regional_ids[[i for i in regional_ids]]


    # set heat demand to 0 if temp_allo_all_regional_ids is higher then 15°C 
    # = heat_norm_masked = heat_norm[temp_allo_all_regional_ids[temp_allo_all_regional_ids > 15].isnull()].fillna(0)
    mask = (temp_allo_all_regional_ids < 15)
    heat_norm_masked = heat_norm.where(mask, other=0)
    heat_norm = heat_norm_masked


    # 17. normalise
    heat_norm = heat_norm.divide(heat_norm.sum(axis=0), axis=1)
    gas_tempinde_norm = gas_temp_inde.divide(gas_temp_inde.sum(axis=0), axis=1)


    return heat_norm, gas_total, gas_tempinde_norm




