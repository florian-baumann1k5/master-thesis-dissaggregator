import pandas as pd

from src.data_processing.heat import *
from src.pipeline.pipe_applications import *
from src.data_processing.cop import *


"""
Assumptions:
    - Political goal: 0 emmisions by 2045 -> linear reduction of gas demand to that year
    - applications: 
        - space_heating/hot_water/process_heat_below_100C:  switch to power(heatpump)
        - process_heat_100_to_200C:                         power(heatpump) and power(electrode)
        - process_heat_200_to_500C:                         power(electrode)
        - process_heat_above_500C:                          H2 (-> no H2 switch for CTS)
        - non_energetic_use:                                H2
"""


# Main function
def temporal_elec_load_from_fuel_switch(year: int, state: str, energy_carrier: str, sector: str, switch_to: str, force_preprocessing: bool = False) -> pd.DataFrame:
    """
    
    """

    p_ground = get_heatpump_distribution()["p_ground"]
    p_air = get_heatpump_distribution()["p_air"]
    p_water = get_heatpump_distribution()["p_water"]


    # 0. validate inputs
    if p_ground + p_air + p_water != 1:
        raise ValueError("sum of percentage of ground/air/water heat pumps must be 1")
    if energy_carrier not in ["gas", "petrol"]:
        raise ValueError("Invalid energy carrier")
    if sector not in ["cts", "industry"]:
        raise ValueError("Invalid sector")
    if switch_to not in ["power", "hydrogen"]:
        raise ValueError("Invalid switch to")
    if sector == "cts" and switch_to == "hydrogen":
        raise ValueError("For CTS all the energy is switched to power!")
    

    # 0.1 get from cache if available
    cache_dir = load_config("base_config.yaml")['temporal_elec_load_from_fuel_switch_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['temporal_elec_load_from_fuel_switch_cache_file'].format(year=year, state=state, energy_carrier=energy_carrier, sector=sector, switch_to=switch_to))
    if os.path.exists(cache_file) and not force_preprocessing:
        logger.info(f"Load temporal_elec_load_from_fuel_switch from cache for year: {year}, state: {state}, sector: {sector}, energy_carrier: {energy_carrier}, switch_to: {switch_to}")
        temporal_fuel_switch =  pd.read_csv(cache_file)
        return temporal_fuel_switch

    

    # selects the correct function to use based on the energy carrier and sector
    if switch_to == "power":
        if energy_carrier == "gas":

            if sector == "cts":
                temporal_fuel_switch = temporal_cts_elec_load_from_fuel_switch_gas(year=year, state=state, switch_to=switch_to)
            elif sector == "industry":
                temporal_fuel_switch = temporal_industry_elec_load_from_fuel_switch_gas(year=year, state=state, switch_to=switch_to)
            else:
                raise ValueError(f"Invalid sector: {sector}")
            
        if energy_carrier == "petrol":
            if sector == "cts":
                temporal_fuel_switch = temporal_cts_elec_load_from_fuel_switch_petrol(year=year, state=state, switch_to=switch_to)
            elif sector == "industry":
                temporal_fuel_switch = temporal_industry_elec_load_from_fuel_switch_petrol(year=year, state=state, switch_to=switch_to)
            else:
                raise ValueError(f"Invalid sector: {sector}")
    elif switch_to == "hydrogen":
        temporal_fuel_switch = hydrogen(year=year, energy_carrier=energy_carrier, state=state)
        
    

    # sanity check
    if temporal_fuel_switch.isna().any().any():
        raise ValueError("DataFrame contains NaN values")
    
    # save to cache
    os.makedirs(cache_dir, exist_ok=True)
    logger.info(f"Save temporal_elec_load_from_fuel_switch to cache for year: {year}, state: {state}, sector: {sector}, energy_carrier: {energy_carrier}, switch_to: {switch_to}")
    temporal_fuel_switch.to_csv(cache_file)    



    return temporal_fuel_switch



# Gas - CTS:
def temporal_cts_elec_load_from_fuel_switch_gas(year: int, state: str, switch_to: str):
    """
    Converts timeseries of gas demand per NUTS-3 and branch and application to
        electric consumption timeseries. Uses COP timeseries for heat
        applications. uses efficiency for mechanical energy.

    Args:
        df_temp_gas_switch : pd.DataFrame()
            timestamp as index, multicolumns with nuts-3, branch and applications.
            contains temporally disaggregated gas demand for fuel switch
        p_ground, p_air, p_water : float, default 0.36, 0.58, 0.06
            percentage of ground/air/water heat pumps sum must be 1

    Returns:
        pd.DataFrame() : timestamp as index, multicolumns with nuts-3, branch and
            applications. temperature dependent and independent profiles from gas
            SLP for temporal disaggregation of df_gas_switch.

    """
    p_ground = get_heatpump_distribution()["p_ground"]
    p_air = get_heatpump_distribution()["p_air"]
    p_water = get_heatpump_distribution()["p_water"]
    energy_carrier = "gas"


    # 0. validate inputs
    if p_ground + p_air + p_water != 1:
        raise ValueError("sum of percentage of ground/air/water heat pumps must be 1")
    
    

    # 1. get gas demand for fuel switch
    sector = "cts"
    df_heat_switch = sector_fuel_switch_fom_gas_petrol(sector=sector, switch_to=switch_to, year=year, energy_carrier=energy_carrier)


    # 2. temporally disaggregate gas demand for fuel switch
    df_temp_heat_switch = disagg_temporal_cts_fuel_switch(df_gas_switch=df_heat_switch, state=state, year=year, energy_carrier=energy_carrier)
    """
    index: timestamp
    columns: [regional_id, industry_sector, application]
    values: [float]: 0
    """



    # 3. calculate total demand with efficiency factors for applications
    df_temp_elec_from_heat_switch = calculate_total_demand_cts(df_temp_gas_switch=df_temp_heat_switch, year=year, energy_carrier=energy_carrier)

    

    return df_temp_elec_from_heat_switch



# Gas -Industry:
def temporal_industry_elec_load_from_fuel_switch_gas(year: int, state: str, switch_to: str):
    """
    Converts timeseries of gas demand per NUTS-3 and branch and application to
        electric consumption timeseries. Uses COP timeseries for heat
        applications. uses efficiency for mechanical energy.

    Args:
        df_temp_gas_switch : pd.DataFrame()
            timestamp as index, multicolumns with nuts-3, branch and applications.
            contains temporally disaggregated gas demand for fuel switch
        p_ground, p_air, p_water : float, default 0.36, 0.58, 0.06
            percentage of ground/air/water heat pumps sum must be 1

    Returns:
        pd.DataFrame() : timestamp as index, multicolumns with nuts-3, branch and
            applications. temperature dependent and independent profiles from gas
            SLP for temporal disaggregation of df_gas_switch.

    """

    p_ground = get_heatpump_distribution()["p_ground"]
    p_air = get_heatpump_distribution()["p_air"]
    p_water = get_heatpump_distribution()["p_water"]
    energy_carrier = "gas"
    
    # 0. validate inputs
    if p_ground + p_air + p_water != 1:
        raise ValueError("sum of percentage of ground/air/water heat pumps must be 1")
    
    

    # 1. get gas demand for fuel switch
    sector = "industry"
    df_gas_switch = sector_fuel_switch_fom_gas_petrol(sector=sector, switch_to=switch_to, year=year, energy_carrier="gas")


    # 2. disaggregate gas demand for fuel switch
    df_temp_gas_switch = disagg_temporal_industry_fuel_switch(df_gas_switch=df_gas_switch, state=state, year=year, energy_carrier=energy_carrier)


    # 4. load fuel switch share for power electrode
    df_electrode = load_fuel_switch_share(sector="industry", switch_to="electrode")
    df_electrode = (df_electrode.loc[[isinstance(x, int) for x in df_electrode["industry_sector"]]].set_index("industry_sector").copy())
    df_electrode.index = df_electrode.index.astype(str)


    # 3. calculate total demand
    df_temp_elec_from_gas_switch = calculate_total_demand_industry(df_temp_gas_switch=df_temp_gas_switch, df_electrode= df_electrode,year=year, energy_carrier=energy_carrier)


    # Drop columns with all zeros
    df_temp_elec_from_gas_switch = df_temp_elec_from_gas_switch.loc[:, (df_temp_elec_from_gas_switch != 0).any(axis=0)]
    
    return df_temp_elec_from_gas_switch


# Petrol - CTS
def temporal_cts_elec_load_from_fuel_switch_petrol(year: int, state: str, switch_to: str):
    """
    Converts timeseries of gas demand per NUTS-3 and branch and application to
        electric consumption timeseries. Uses COP timeseries for heat
        applications. uses efficiency for mechanical energy.

    Args:
        df_temp_gas_switch : pd.DataFrame()
            timestamp as index, multicolumns with nuts-3, branch and applications.
            contains temporally disaggregated gas demand for fuel switch
        p_ground, p_air, p_water : float, default 0.36, 0.58, 0.06
            percentage of ground/air/water heat pumps sum must be 1

    Returns:
        pd.DataFrame() : timestamp as index, multicolumns with nuts-3, branch and
            applications. temperature dependent and independent profiles from gas
            SLP for temporal disaggregation of df_gas_switch.

    """


    p_ground = get_heatpump_distribution()["p_ground"]
    p_air = get_heatpump_distribution()["p_air"]
    p_water = get_heatpump_distribution()["p_water"]



    # 0. validate inputs
    if p_ground + p_air + p_water != 1:
        raise ValueError("sum of percentage of ground/air/water heat pumps must be 1")
    
    
    # set variables
    sector = "cts"
    energy_carrier = "petrol"
    

    # 1. get gas demand for fuel switch
    df_petrol_switch = sector_fuel_switch_fom_gas_petrol(sector=sector, switch_to=switch_to, year=year, energy_carrier=energy_carrier)


    # 2. disaggregate petrol demand for fuel switch
    df_temp_petrol_switch = disagg_temporal_cts_fuel_switch(df_gas_switch=df_petrol_switch, state=state, year=year, energy_carrier=energy_carrier)


    # 3. calculate total demand
    df_temp_elec_from_petrol_switch = calculate_total_demand_cts(df_temp_gas_switch=df_temp_petrol_switch, year=year, energy_carrier=energy_carrier)

   
    return df_temp_elec_from_petrol_switch



# Petrol - Industry
def temporal_industry_elec_load_from_fuel_switch_petrol(year: int, state: str, switch_to: str):
    """
    Converts timeseries of gas demand per NUTS-3 and branch and application to
        electric consumption timeseries. Uses COP timeseries for heat
        applications. uses efficiency for mechanical energy.

    Args:
        df_temp_gas_switch : pd.DataFrame()
            timestamp as index, multicolumns with nuts-3, branch and applications.
            contains temporally disaggregated gas demand for fuel switch
        p_ground, p_air, p_water : float, default 0.36, 0.58, 0.06
            percentage of ground/air/water heat pumps sum must be 1

    Returns:
        pd.DataFrame() : timestamp as index, multicolumns with nuts-3, branch and
            applications. temperature dependent and independent profiles from gas
            SLP for temporal disaggregation of df_gas_switch.

    """

    p_ground = get_heatpump_distribution()["p_ground"]
    p_air = get_heatpump_distribution()["p_air"]
    p_water = get_heatpump_distribution()["p_water"]
    energy_carrier = "petrol"
    
    # 0. validate inputs
    if p_ground + p_air + p_water != 1:
        raise ValueError("sum of percentage of ground/air/water heat pumps must be 1")
    
    

    # 1. get gas demand for fuel switch
    sector = "industry"
    df_gas_switch = sector_fuel_switch_fom_gas_petrol(sector=sector, switch_to=switch_to, year=year, energy_carrier="gas")


    # 2. disaggregate gas demand for fuel switch
    df_temp_gas_switch = disagg_temporal_industry_fuel_switch(df_gas_switch=df_gas_switch, state=state, year=year, energy_carrier=energy_carrier)


    # 4. load fuel switch share for power electrode
    df_electrode = load_fuel_switch_share(sector="industry", switch_to="electrode")
    df_electrode = (df_electrode.loc[[isinstance(x, int) for x in df_electrode["industry_sector"]]].set_index("industry_sector").copy())
    df_electrode.index = df_electrode.index.astype(str)


    # 3. calculate total demand
    df_temp_elec_from_gas_switch = calculate_total_demand_industry(df_temp_gas_switch=df_temp_gas_switch, df_electrode= df_electrode,year=year, energy_carrier=energy_carrier)


    # Drop columns with all zeros
    df_temp_elec_from_gas_switch = df_temp_elec_from_gas_switch.loc[:, (df_temp_elec_from_gas_switch != 0).any(axis=0)]
    
    return df_temp_elec_from_gas_switch











# hydrogen after switch
def hydrogen(year: int, energy_carrier: str, state: str) -> pd.DataFrame:
    """
    Determines hydrogen consumption to replace gas consumption.
    """
    
    df_gas_switch = sector_fuel_switch_fom_gas_petrol(sector="industry", switch_to="hydrogen", year=year, energy_carrier=energy_carrier)


    df_temp_gas_switch = disagg_temporal_industry_fuel_switch(df_gas_switch=df_gas_switch, state=state, year=year, energy_carrier=energy_carrier)

    df_hydro = hydrogen_after_switch(df_gas_switch=df_temp_gas_switch, energy_carrier=energy_carrier)

    # Remove columns with only zeros
    df_hydro = df_hydro.loc[:, (df_hydro != 0).any(axis=0)]
    return df_hydro



""" aus 12. Hydrogen aus 05_Demo_CTS_Industry_disaggregation_applications_results_flo.ipynb 
def strombedarf_electrolyse = hydrogen_after_switch / 0.7
    -> das ist derstrombedarf dafür draufgeht mit strom den H2 zu erzeugen



def disagg_temporal_applications_hp
    -> create_hp_load(

"""


# Gas & Petrol 
# calculate the gas that has to be switched to Power/ H2
def sector_fuel_switch_fom_gas_petrol(sector: str, switch_to: str, year: int, energy_carrier: str, force_preprocessing: bool = False) -> pd.DataFrame:
    """
    Determines yearly gas/petrol demand per branch and regional id for heat applications
    that will be replaced by power or hydrogen in the future.

    Assumptions:
        - Political goal: 0 emissions by 2045 -> linear reduction of gas demand to that year
        - applications: 
            - space_heating/hot_water/process_heat_below_100C:  switch to power(heatpump)
            - process_heat_100_to_200C:                         power(heatpump) and power(electrode)
            - process_heat_200_to_500C:                         power(electrode)
            - process_heat_above_500C:                          H2 (-> not in CTS -> no H2 switch for CTS)
            - non_energetic_use:                                H2 (-> not in CTS -> no H2 switch for CTS)

    Args:
        sector : str
            must be one of ['cts', 'industry']
        switch_to: str
            must be one of ['power', 'hydrogen']
        energy_carrier: str
            must be one of ['gas', 'petrol']
    Returns:
        pd.DataFrame:
            index: regional_id (all 400)
            columns: [industry_sector, application]
            values: gas/petrol demand that needs to be replaced by power or hydrogen

    """

    # 0. validate inputs
    if sector not in ['cts', 'industry']:
        raise ValueError(f"Invalid sector: {sector}")
    if switch_to not in ['power', 'hydrogen']:
        raise ValueError(f"Invalid switch_to: {switch_to}")
    if energy_carrier not in ['gas', 'petrol']:
        raise ValueError(f"Invalid energy_carrier: {energy_carrier}")
    if sector == "cts" and switch_to == "hydrogen":
        raise ValueError(f"For CTS all the energy is switched to power!")

    # Define cache directory and file path
    cache_dir = load_config("base_config.yaml")['temporal_elec_load_from_fuel_switch_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['temporal_elec_load_from_fuel_switch_cache_file'].format(year=year, sector=sector, switch_to=switch_to, energy_carrier=energy_carrier))

    # Check if cache exists and load if available
    if os.path.exists(cache_file) and not force_preprocessing:
        logger.info(f"Loading cached data for year: {year}, sector: {sector}, switch_to: {switch_to}, energy_carrier: {energy_carrier}")
        return pd.read_csv(cache_file, index_col=0, header=[0, 1])

    # 1. load consumption data by application and wz and region
    df_consumption = disagg_applications_efficiency_factor(year=year, energy_carrier=energy_carrier, sector=sector)
    df_consumption.columns = pd.MultiIndex.from_tuples(
        [(str(b), str(a)) for b, a in df_consumption.columns],
        names=df_consumption.columns.names
    )


    # 1. load data
    df_fuel_switch = get_fuel_switch_share(sector=sector, switch_to=switch_to)

    # 2. project fuel switch share to year (0 by 2045 - political goal; linear interpolation)
    fuel_switch_projected = projection_fuel_switch_share(df_fuel_switch=df_fuel_switch, target_year=year)
    fuel_switch_projected.index = fuel_switch_projected.index.map(str)
    fuel_switch_projected.columns = fuel_switch_projected.columns.map(str)
    
    fs_stacked = fuel_switch_projected.stack(dropna=True)
    multiplier_series = fs_stacked.reindex(df_consumption.columns, fill_value=0)


    # 5. multiply the fuel switch share with the consumption data
    df_fossil_switch = pd.DataFrame(index=df_consumption.index, columns=df_consumption.columns, data=0)
    df_fossil_switch = df_consumption * multiplier_series


    # 6. Drop columns with all zeros - 
    df_fossil_switch = df_fossil_switch.loc[:, ~(df_fossil_switch == 0).all()]

    # Save to cache
    os.makedirs(cache_dir, exist_ok=True)
    df_fossil_switch.to_csv(cache_file)
    logger.info(f"Data cached for year: {year}, sector: {sector}, switch_to: {switch_to}, energy_carrier: {energy_carrier}")

    return df_fossil_switch

# temaporal disaggregation of gas/petrol demand
def disagg_temporal_cts_fuel_switch(df_gas_switch: pd.DataFrame, state: str, year: int, energy_carrier: str) -> pd.DataFrame:
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
            columns[0]: regional_id
            columns[1]: industry_sector
            columns[2]: application
            values: [float]: 0

        
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
    heat_norm_1h, consumption_total, gas_tempinde_norm_1h = create_heat_norm_cts(state=state, year=year, energy_carrier=energy_carrier)



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
    heat_norm_15min_uncomplete = (heat_norm_1h
        .resample('15min')
        .asfreq()
        .interpolate(method='linear', limit_direction='forward', axis=0)
    )
    extension = pd.DataFrame(
        index=pd.date_range(
            heat_norm_15min_uncomplete.index[-1:].values[0],
            periods=4,
            freq='15min'
        )[-3:],
        columns=heat_norm_15min_uncomplete.columns
    )
    heat_norm_15min = pd.concat([heat_norm_15min_uncomplete, extension]).ffill()

    # normalize over complete year
    heat_norm_15min = heat_norm_15min.divide(heat_norm_15min.sum(), axis=1)
    heat_norm_15min = heat_norm_15min.fillna(0.0)


    # 4.gas_tempinde_norm: transform it into a 15-minute resolution using interpolation and normalize
    gas_tempinde_norm_15min_uncomplete = (gas_tempinde_norm_1h
        .resample('15min').asfreq()
        .interpolate(method='linear', limit_direction='forward', axis=0))
    extension = pd.DataFrame(
        index=pd.date_range(gas_tempinde_norm_15min_uncomplete.index[-1:]
            .values[0],
            periods=4,
            freq='15min'
        )[-3:],
        columns=gas_tempinde_norm_15min_uncomplete.columns)
    gas_tempinde_norm_15min = pd.concat([gas_tempinde_norm_15min_uncomplete, extension]).ffill()
    # normalize over complete year
    gas_tempinde_norm_15min = gas_tempinde_norm_15min.divide(gas_tempinde_norm_15min.sum(), axis=1)
    gas_tempinde_norm_15min = gas_tempinde_norm_15min.fillna(0.0)



    # 5. create temp disaggregated gas demands per nuts-3, branch and app
    all_regional_ids = new_df.columns.get_level_values(0).unique()
    for regional_id in all_regional_ids:

        df_switch_region = df_gas_switch.loc[regional_id]
        for industry_sector in df_switch_region.index.get_level_values(0).unique():
            
            df_switch_branch = df_switch_region.loc[industry_sector]
            for app in df_switch_branch.index:

                # space heating will be handled with the temperature dependent profile, the others with a general profile
                if app == 'space_heating':
                    new_df[regional_id, industry_sector, app] = ((df_gas_switch.loc[regional_id][industry_sector, app]) * (heat_norm_15min[regional_id, int(industry_sector)]))
                else:
                    new_df[regional_id, industry_sector, app] = ((df_gas_switch.loc[regional_id][industry_sector, app]) * (gas_tempinde_norm_15min[regional_id, int(industry_sector)]))
        

    # 6. drop all columns that have only nan values
    new_df.dropna(axis=1, how='all', inplace=True)


    return new_df

def disagg_temporal_industry_fuel_switch(df_gas_switch: pd.DataFrame, state: str, year: int, energy_carrier: str, low: float = 0.5) -> pd.DataFrame:
    """
    Temporally disaggregates industry gas demand, which will be switched to
    electricity or hydrogen, by state.

    Parameters
    -------
    df_gas_switch : pd.DataFrame
        Gas demand by branch, application and NUTS-3 region which will be
        replaced.
    state : str, default None
        Specifies state. Must by one of the entries of bl_dict().values(),
        ['SH', 'HH', 'NI', 'HB', 'NW', 'HE', 'RP', 'BW', 'BY', 'SL', 'BE',
         'BB', 'MV', 'SN', 'ST', 'TH']
    Returns
    -------
    pd.DataFrame() : timestamp as index, multicolumns with nuts-3, branch and
        applications. uses shift load profiles for temporal disaggregation
        of df_gas_switch.

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
    heat_norm, gas_total, gas_tempinde_norm = create_heat_norm_industry(state=state, year=year, energy_carrier=energy_carrier)


    # 3. get shift load profiles
    # troughput values for the helper function, used for industrial disagg
    sp_bl = get_shift_load_profiles_by_state_and_year(state=state, low=low, year=year)


    # 4. create time index
    time_index_15min = pd.date_range(start=f"{year}", end=f"{year + 1}", freq="15T")[:-1]



    # 5. get normalized timeseries for temperature dependent gas demand for
    # industrial indoor heating, approximated with cts indoor heat with gas SLP 'KO'
    if 'space_heating' in df_gas_switch.columns.unique(level=1):
        # upsample heat_norm to quarter hours and interpolate, then normalize
        heat_norm = (heat_norm.resample('15T').asfreq().interpolate(method='linear', limit_direction='forward', axis=0))
        # extend DataFrame by 3 more periods
        extension = pd.DataFrame(index=pd.date_range(heat_norm.index[-1:].values[0], periods=4, freq='15T')[-3:], columns=heat_norm.columns)
        heat_norm = pd.concat([heat_norm, extension]).fillna(method='ffill')
        # normalize
        heat_norm = heat_norm.divide(heat_norm.sum(), axis=1)
        assert heat_norm.index.equals(time_index_15min), "The time-indizes are not aligned"



    # start assigning disaggregated demands to columns of new_df by multiplying
    # shift_load_profiles with yearly demands per region, branch and app
    assert sp_bl.index.equals(time_index_15min), "The time-indizes are not aligned"
    # nuts-3 (lk) per state


    # 5. get shift load profiles for industry
    shift_profile_industry_dict = shift_profile_industry()


    # 5. create temp disaggregated gas demands per nuts-3, branch and app
    for regional_id in new_df.columns.get_level_values(0).unique():

        for industry_sector in (df_gas_switch.loc[regional_id].index.get_level_values(0).unique()):
            
            for app in df_gas_switch.loc[regional_id][industry_sector].index:

                if app == 'space_heating':
                    new_df[regional_id, industry_sector, app] = ((df_gas_switch.loc[regional_id][industry_sector, app]) * (heat_norm[regional_id]))
                else:
                    new_df.loc[:, (regional_id, industry_sector, app)] = (df_gas_switch.loc[regional_id][industry_sector, app] * sp_bl[shift_profile_industry_dict[int(industry_sector)]])
        

    # drop all columns with only zeros
    new_df = new_df.loc[:, (new_df != 0).any(axis=0)]



    return new_df