import logging
import pandas as pd
import holidays
import datetime

from src import logger
from src.data_access.local_reader import *
from src.pipeline.pipe_applications import *
from src.utils.utils import *
from src.data_processing.temperature import *
from src.data_processing.consumption import *

def disaggregate_temporal_industry(consumption_data: pd.DataFrame, year: int, low=0.5) -> pd.DataFrame:
    """
    Calculates the temporal distribution of industrial energy consumption for a
    given energy carrier and year using standard load profiles. 

    "consumption_data" having the consumption by industry sector and regional id
        Index = regional_ids
        Columns = industry_sectors

    "slp" having the shift load profiles by state and year
        Index: date time timestamps of the year in 15min e.g. 2015-01-01 02:15:00
        Multicolumns: ["state", "loadprofile"]      

    
    Args:
        energy_carrier: The energy carrier (e.g., 'power').
        year: The year for the analysis.
        low: Parameter for getting shift load profiles (default 0.5).

    Returns:
        A DataFrame (35040 rows x 11600 columns) with:
            - index: datetime timestamps
            - columns: MultiColumn ['regional_id', 'industry_sector']
        containing the disaggregated consumption time series in 15-min intervals.

    Raises:
        ValueError: If a NaN value is found in the consumption data after stacking.
    """

    logger.info(f"Starting disaggregate_temporal_industry for year: {year}")

    # 1. Get consumption data for industry sector and make sure the columns are strings
    consumption_data.columns = consumption_data.columns.astype(str)

    # 1.2. calculate the total consumption for plausalilty check
    total_consumption_start = consumption_data.sum().sum()


    # 2. Get Standard Load Profiles
    slp = get_shift_load_profiles_by_year(year=year, low=low, force_preprocessing=False)
    slp.index = pd.to_datetime(slp.index)


    # 3. Perform Disaggregation (Integrated Logic)
    state_mapping = federal_state_dict()
    profile_mapping = shift_profile_industry()
    disaggregated_results = {}


    # 4. Filter consumption columns
    industry_cols = []
    non_industry_cols = []
    for col in consumption_data.columns:
        try:
            int_col = int(col)
            if int_col in profile_mapping:
                 industry_cols.append(col)
            else:
                 non_industry_cols.append(col)
        except ValueError:
            non_industry_cols.append(col)

    if non_industry_cols:
        logger.info(f"Info: Excluding non-industry/unmapped columns: {non_industry_cols}")
    if not industry_cols:
         logger.error("Error: No valid industry sector columns found in consumption_data.")


    consumption_data_industries = consumption_data[industry_cols]
    consumption_stacked = consumption_data_industries.stack()
    logger.info(f"Processing {len(consumption_stacked)} regional/industry combinations...")


    # 5. Iterate through combinations
    processed_count = 0
    error_count = 0 # Counts errors leading to skipping
    for (regional_id, industry_sector_str), annual_consumption in consumption_stacked.items():

        # Check specifically for NaN values and raise an error (Processing continues if annual_consumption is 0.0 or positive)
        if pd.isna(annual_consumption):
            error_count += 1 # Increment count before raising
            # Raise error immediately - stops the whole process
            raise ValueError(f"NaN value found for annual_consumption at "
                             f"index ({regional_id}, '{industry_sector_str}'). "
                             f"Processing cannot continue with NaN values.")
        try:
            # Proceed with disaggregation logic (this now includes 0.0 values)
            state_num = int(regional_id) // 1000
            state_abbr = state_mapping[state_num]
            industry_sector_int = int(industry_sector_str)
            load_profile_name = profile_mapping[industry_sector_int]
            profile_series = slp[(state_abbr, load_profile_name)]

            # Multiply profile by consumption (if 0.0, result is Series of zeros)
            disaggregated_series = profile_series * annual_consumption

            disaggregated_results[(regional_id, industry_sector_int)] = disaggregated_series
            processed_count += 1

        except KeyError as e:
             # Handle missing keys in mappings or SLP columns
             if e.args[0] == state_num:
                  errmsg = f"state number {state_num} (from region {regional_id}) not found in state_mapping"
             elif e.args[0] == industry_sector_int:
                  errmsg = f"industry sector {industry_sector_int} not found in profile_mapping"
             elif isinstance(e.args[0], tuple) and e.args[0] == (state_abbr, load_profile_name):
                   errmsg = f"SLP column for ({state_abbr}, {load_profile_name}) not found"
             else:
                   errmsg = f"Mapping/Selection key not found: {e}"
             logger.warning(f"Warning: Skipping combination ({regional_id}, {industry_sector_str}). {errmsg}")
             error_count += 1
        except Exception as e:
            # Catch other unexpected errors during calculation
            logger.warning(f"Warning: An unexpected error occurred for combination ({regional_id}, {industry_sector_str}): {e}")
            error_count += 1


    logger.info(f"Disaggregation loop finished. Processed (incl. zeros): {processed_count}, Errors/Skipped: {error_count}")

    # Combine results (includes columns with zeros if annual_consumption was 0)
    if not disaggregated_results:
        logger.warning("Warning: No data was successfully processed. Resulting DataFrame will be empty.")
        empty_cols = pd.MultiIndex(levels=[[],[]], codes=[[],[]], names=['regional_id', 'industry_sector'])
        return pd.DataFrame(index=slp.index, columns=empty_cols)

    final_df = pd.DataFrame(disaggregated_results)
    final_df.columns.names = ['regional_id', 'industry_sector']

    # 6. calculate the total consumption for plausalilty check
    total_consumption_end = final_df.sum().sum()
    if not np.isclose(total_consumption_end, total_consumption_start):
        raise ValueError("Warning: Total consumption is not the same as the start! "
                         f"total_consumption_start: {total_consumption_start}, "
                         f"total_consumption_end: {total_consumption_end}")

    return final_df


def disagg_temporal_gas_CTS(consumption_data: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Disaggregates the temporal distribution of gas consumption for CTS in a given year.

    Args:
        consumption_data: DataFrame containing consumption data with columns ['regional_id', 'industry_sector']
        year: The year for the analysis
    """


    # 1. get the number of hours in the year
    hours_of_year = get_hours_of_year(year)


    # 2. get the temperature allocation
    daily_temperature_allocation = allocation_temperature(year=year)


    # TODO refactor 
    # disagg_daily_gas_slp_cts(state, temperatur_df, year=year)
    df =  disagg_daily_gas_slp_cts(gas_consumption=consumption_data, state="BW", temperatur_df=daily_temperature_allocation, year=year)


    

    #....

    return None









def get_shift_load_profiles_by_state_and_year(state: str, low: float = 0.5, year: int = 2015): 

    """
    Return shift load profiles in normalized units
    ('normalized' means that the sum over all time steps equals to one).

    DISS 4.4.1
    = shift_load_profile_generator()

    Args:
        state : str
            Must be one of ['BW', 'BY', 'BE', 'BB', 'HB', 'HH', 'HE', 'MV',
                            'NI', 'NW', 'RP', 'SL', 'SN', 'ST', 'SH', 'TH']
        low : float
            Load level during "low" loads. Industry loads have two levels:
                "low" outside of working hours and "high" during working hours.
            Default is set to 0.35 for low, which was deduced from real load data.

    Returns:
        pd.DataFrame:
            Shift load profiles for the given state and year.
            index: "Date": the year in 15min steps in datetime format(2015-01-01 06:00:00)
            columns: 'S1_WT', 'S1_WT_SA', 'S1_WT_SA_SO', 'S2_WT', 'S2_WT_SA', 'S2_WT_SA_SO', 'S3_WT', 'S3_WT_SA', 'S3_WT_SA_SO'
    """

    # 0. validate input
    if state not in ['BW', 'BY', 'BE', 'BB', 'HB', 'HH', 'HE', 'MV',
                        'NI', 'NW', 'RP', 'SL', 'SN', 'ST', 'SH', 'TH']:
        raise ValueError(f"Invalid state: {state}")

    # 1. Create datetime index for the full year in 15-minute steps
    idx = pd.date_range(start=f'{year}-01-01', end=f'{year+1}-01-01', freq='15min')[:-1]    # Build DataFrame and extract features using .dt accessors (faster + cleaner)
    df = pd.DataFrame({'Date': idx})
    df['Day'] = df['Date'].dt.date
    df['Hour'] = df['Date'].dt.time
    df['DayOfYear'] = df['Date'].dt.dayofyear
    # Store number of periods
    periods = len(df) # = number of 15min takts in the year
    


    # 2. create holiday mask
    # Extract all holiday dates for the state and year
    holiday_dates = holidays.DE(state=state, years=year).keys()
    # Create a boolean mask for rows in df where 'Day' is a holiday
    hd = df['Day'].isin(holiday_dates)
    


    # 3. create weekday mask
    # Get weekday as integer (0=Mon, ..., 6=Sun)
    weekday = df['Date'].dt.weekday
    # Mark workdays (Mon-Fri) that are not holidays
    df['workday'] = (weekday < 5) & (~hd)
    # Saturdays, excluding holidays
    df['saturday'] = (weekday == 5) & (~hd)
    # Sundays or any holiday
    df['sunday'] = (weekday == 6) | hd
    # 24th and 31st of december are treated like a saturday
    special_days = {datetime.date(year, 12, 24), datetime.date(year, 12, 31)}
    special_mask = df['Day'].isin(special_days)
    # Set all other weekday flags to False for these special days
    df.loc[special_mask, ['workday', 'sunday']] = False
    df.loc[special_mask, 'saturday'] = True
    


    # 4. create shift load profiles
    for sp in ['S1_WT', 'S1_WT_SA', 'S1_WT_SA_SO', 'S2_WT', 'S2_WT_SA', 'S2_WT_SA_SO', 'S3_WT', 'S3_WT_SA', 'S3_WT_SA_SO']:
        if(sp == 'S1_WT'):
            anzahl_wz = 17 / 48 * len(df[df['workday']])
            anzahl_nwz = (31 / 48 * len(df[df['workday']]) + len(df[df['sunday']]) + len(df[df['saturday']]))
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = (df['sunday'] | df['saturday'])
            df.loc[mask, sp] = low * anteil
            mask = ((df['workday']) & ((df['Hour'] < pd.to_datetime('08:00:00').time()) | (df['Hour'] >= pd.to_datetime('16:30:00').time())))
            df.loc[mask, sp] = low * anteil
    
        elif(sp == 'S1_WT_SA'):
            anzahl_wz = (17 / 48 * len(df[df['workday']]) + 17 / 48 * len(df[df['saturday']]))
            anzahl_nwz = (31 / 48 * len(df[df['workday']]) + len(df[df['sunday']]) + 31/48 * len(df[df['saturday']]))
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = df['sunday']
            df.loc[mask, sp] = low * anteil
            mask = ((df['workday']) & ((df['Hour'] < pd.to_datetime('08:00:00').time()) | (df['Hour'] >= pd.to_datetime('16:30:00').time())))
            df.loc[mask, sp] = low * anteil
            mask = ((df['saturday']) & ((df['Hour'] < pd.to_datetime('08:00:00').time()) | (df['Hour'] >= pd.to_datetime('16:30:00').time())))
            df.loc[mask, sp] = low * anteil
        
        elif(sp == 'S1_WT_SA_SO'):
            anzahl_wz = (17 / 48 * (len(df[df['workday']]) + len(df[df['sunday']]) + len(df[df['saturday']])))
            anzahl_nwz = (31 / 48 * (len(df[df['workday']]) + len(df[df['sunday']]) + len(df[df['saturday']])))
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = ((df['Hour'] < pd.to_datetime('08:00:00').time()) | (df['Hour'] >= pd.to_datetime('16:30:00').time()))
            df.loc[mask, sp] = low * anteil
        
        elif(sp == 'S2_WT'):
            anzahl_wz = 17/24 * len(df[df['workday']])
            anzahl_nwz = (7/24 * len(df[df['workday']]) + len(df[df['sunday']]) + len(df[df['saturday']]))
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = (df['sunday'] | df['saturday'])
            df.loc[mask, sp] = low * anteil
            mask = ((df['workday']) & ((df['Hour'] < pd.to_datetime('06:00:00').time()) | (df['Hour'] >= pd.to_datetime('23:00:00').time())))
            df.loc[mask, sp] = low * anteil
        
        elif(sp == 'S2_WT_SA'):
            anzahl_wz = 17/24 * (len(df[df['workday']]) + len(df[df['saturday']]))
            anzahl_nwz = (7/24 * len(df[df['workday']]) + len(df[df['sunday']]) + 7/24 * len(df[df['saturday']]))
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = df['sunday']
            df.loc[mask, sp] = low * anteil
            mask = (((df['workday']) | (df['saturday'])) & ((df['Hour'] < pd.to_datetime('06:00:00').time()) | (df['Hour'] >= pd.to_datetime('23:00:00').time())))
            df.loc[mask, sp] = low * anteil
        
        elif(sp == 'S2_WT_SA_SO'):
            anzahl_wz = (17/24 * (len(df[df['workday']]) + len(df[df['saturday']]) + len(df[df['sunday']])))
            anzahl_nwz = (7/24 * (len(df[df['workday']]) + len(df[df['sunday']]) + len(df[df['saturday']])))
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = (((df['Hour'] < pd.to_datetime('06:00:00').time()) | (df['Hour'] >= pd.to_datetime('23:00:00').time())))
            df.loc[mask, sp] = low * anteil
        
        elif(sp == 'S3_WT_SA_SO'):
            anteil = 1 / periods
            df[sp] = anteil
        
        elif(sp == 'S3_WT'):
            anzahl_wz = len(df[df['workday']])
            anzahl_nwz = len(df[df['sunday']]) + len(df[df['saturday']])
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = (df['sunday'] | df['saturday'])
            df.loc[mask, sp] = low * anteil
        
        elif(sp == 'S3_WT_SA'):
            anzahl_wz = len(df[df['workday']]) + len(df[df['saturday']])
            anzahl_nwz = len(df[df['sunday']])
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = df['sunday']
            df.loc[mask, sp] = low * anteil
    
    
    df = (df[['Date', 'S1_WT', 'S1_WT_SA', 'S1_WT_SA_SO', 'S2_WT', 'S2_WT_SA', 'S2_WT_SA_SO', 'S3_WT', 'S3_WT_SA', 'S3_WT_SA_SO']]
          .set_index('Date'))
    return df


def get_CTS_power_slp(state, year: int):
    """
    Return the electric standard load profiles in normalized units
    ('normalized' means here that the sum over all time steps equals one).
    DISS 4.4.1

    Parameters
    ----------
    state: str
        must be one of ['BW', 'BY', 'BE', 'BB', 'HB', 'HH', 'HE', 'MV',
                        'NI', 'NW', 'RP', 'SL', 'SN',' ST', 'SH', 'TH']

    Returns
    -------
    pd.DataFrame
    """
    def Leistung(Tag_Zeit, mask, df, df_SLP):
        u = pd.merge(df[mask], df_SLP[['Hour', Tag_Zeit]], on=['Hour'], how='left')
        v = pd.merge(df, u[['Date', Tag_Zeit]], on=['Date'], how='left')
        v_filled = v.fillna(0.0)
        v_filled = v_filled.infer_objects(copy=False)
        return v_filled[Tag_Zeit]


    idx = pd.date_range(start=str(year), end=str(year+1), freq='15min')[:-1]
    df = (pd.DataFrame(data={'Date': idx})
            .assign(Day=lambda x: pd.DatetimeIndex(x['Date']).date)
            .assign(Hour=lambda x: pd.DatetimeIndex(x['Date']).time)
            .assign(DayOfYear=lambda x:
                    pd.DatetimeIndex(x['Date']).dayofyear.astype(int)))
    
    mask_holidays = []
    for i in range(0, len(holidays.DE(state=state, years=year))):
        mask_holidays.append('Null')
        mask_holidays[i] = ((df['Day'] == [x for x in holidays.DE(state=state, years=year).items()][i][0]))


    hd = mask_holidays[0]
    
    for i in range(1, len(holidays.DE(state=state, years=year))):
        hd = hd | mask_holidays[i]
    
    df['WD'] = df['Date'].apply(lambda x: x.weekday() < 5) & (~hd)
    df['SA'] = df['Date'].apply(lambda x: x.weekday() == 5) & (~hd)
    df['SU'] = df['Date'].apply(lambda x: x.weekday() == 6) | hd
    
    mask = df['Day'].isin([datetime.date(year, 12, 24), datetime.date(year, 12, 31)])

    df.loc[mask, ['WD', 'SU']] = False
    df.loc[mask, 'SA'] = True


    wiz1 = df.loc[df['Date'] < (str(year) + '-03-21 00:00:00')]
    wiz2 = df.loc[df['Date'] >= (str(year) + '-11-01')]


    soz = (df.loc[((str(year) + '-05-15') <= df['Date'])  & (df['Date'] < (str(year) + '-09-15'))])
    uez1 = (df.loc[((str(year) + '-03-21') <= df['Date'])  & (df['Date'] < (str(year) + '-05-15'))])
    uez2 = (df.loc[((str(year) + '-09-15') <= df['Date']) & (df['Date'] <= (str(year) + '-10-31'))])


    df = df.assign(WIZ=lambda x: (x.Day.isin(wiz1.Day) | x.Day.isin(wiz2.Day)),
                   SOZ=lambda x: x.Day.isin(soz.Day),
                   UEZ=lambda x: (x.Day.isin(uez1.Day) | x.Day.isin(uez2.Day)))

    last_strings = []


    # SLPs: H= Haushalt, L= Landwirtschaft, G= Gewerbe
    for profile in ['H0', 'L0', 'L1', 'L2', 'G0', 'G1', 'G2', 'G3', 'G4', 'G5', 'G6']:
        
        df_load = load_power_load_profile(profile)

        df_load.columns = ['Hour', 'SA_WIZ', 'SU_WIZ', 'WD_WIZ', 'SA_SOZ',
                           'SU_SOZ', 'WD_SOZ', 'SA_UEZ', 'SU_UEZ', 'WD_UEZ']
        df_load.loc[1] = df_load.loc[len(df_load) - 2]
        df_SLP = df_load[1:97]
        df_SLP = df_SLP.reset_index()[['Hour', 'SA_WIZ', 'SU_WIZ', 'WD_WIZ',
                                       'SA_SOZ', 'SU_SOZ', 'WD_SOZ', 'SA_UEZ',
                                       'SU_UEZ', 'WD_UEZ']]
        wd_wiz = Leistung('WD_WIZ', (df.WD & df.WIZ), df, df_SLP)
        wd_soz = Leistung('WD_SOZ', (df.WD & df.SOZ), df, df_SLP)
        wd_uez = Leistung('WD_UEZ', (df.WD & df.UEZ), df, df_SLP)
        sa_wiz = Leistung('SA_WIZ', (df.SA & df.WIZ), df, df_SLP)
        sa_soz = Leistung('SA_SOZ', (df.SA & df.SOZ), df, df_SLP)
        sa_uez = Leistung('SA_UEZ', (df.SA & df.UEZ), df, df_SLP)
        su_wiz = Leistung('SU_WIZ', (df.SU & df.WIZ), df, df_SLP)
        su_soz = Leistung('SU_SOZ', (df.SU & df.SOZ), df, df_SLP)
        su_uez = Leistung('SU_UEZ', (df.SU & df.UEZ), df, df_SLP)
        Summe = (wd_wiz + wd_soz + wd_uez + sa_wiz + sa_soz + sa_uez
                 + su_wiz + su_soz + su_uez)
        Last = 'Last_' + str(profile)
        last_strings.append(Last)
        df[Last] = Summe
        total = sum(df[Last])
        df_normiert = df[Last] / total
        df[profile] = df_normiert

    return df.drop(columns=last_strings).set_index('Date')


def get_shift_load_profiles_by_year(year: int, low: float = 0.5, force_preprocessing: bool = False):
    """
    Return the shift load profiles for a given year.
    The sum of every column (state, load_profile) equals 1.

    Args:
        year (int): The year to get the shift load profiles for.
        low (float): The low load level.
        force_preprocessing (bool): Whether to force preprocessing.

    Returns:
        pd.DataFrame: The shift load profiles for the given year. MultiIndex columns: [state, shift_load_profile]
    """

    # 0. validate input
    if year < 2000 or year > 2050:
        raise ValueError("Year must be between 2000 and 2050")

    # 1. load from cache if not force_preprocessing and cache exists
    if not force_preprocessing:
         shift_load_profiles = load_shift_load_profiles_by_year_cache(year=year)

         if shift_load_profiles is not None:
            return shift_load_profiles
    
    # 2. get states
    states = federal_state_dict().values()

    df_list = []

    # 3. get shift load profiles for each state
    for state in states:
        slp = get_shift_load_profiles_by_state_and_year(state=state, year=year, low=low)
        
        # 3.1 Set MultiIndex on columns: [<state>, <slp_column>]
        slp.columns = pd.MultiIndex.from_product([[state], slp.columns])
        df_list.append(slp)

    # 4. Concatenate all SLPs horizontally
    combined_slp = pd.concat(df_list, axis=1)

    # 5. save to cache
    processed_dir = load_config("base_config.yaml")['shift_load_profiles_cache_dir']
    processed_file = os.path.join(processed_dir, load_config("base_config.yaml")['shift_load_profiles_cache_file'].format(year=year))
    os.makedirs(processed_dir, exist_ok=True)
    combined_slp.to_csv(processed_file)    



def gas_slp_weekday_params(state: int, year: int):
    """
    Return the weekday-parameters of the gas standard load profiles

    Args:
        state: str
            must be one of ['BW', 'BY', 'BE', 'BB', 'HB', 'HH', 'HE', 'MV',
                            'NI', 'NW', 'RP', 'SL', 'SN',' ST', 'SH', 'TH']
        year: int
    
    
    Returns:
        pd.DataFrame:
            index: daytime for every day in the year
            columns: 
                ['MO', 'DI', 'MI', 'DO', 'FR', 'SA', 'SO']: containing true if the day of the year is that day
                ['FW_<slp_name>']: SLP values see  dict gas_load_profile_parameters_dict()
    """


    

    idx = pd.date_range(start=str(year), end=str(year+1), freq='d')[:-1]
    df = (pd.DataFrame(data={'Date': idx})
            .assign(Day=lambda x: pd.DatetimeIndex(x['Date']).date)
            .assign(DayOfYear=lambda x:
                    pd.DatetimeIndex(x['Date']).dayofyear.astype(int)))


    mask_holiday = []
    for i in range(0, len(holidays.DE(state=state, years=year))):
        mask_holiday.append('Null')
        mask_holiday[i] = ((df['Day'] == [x for x in holidays.DE(state=state,
                                          years=year).items()][i][0]))
    hd = mask_holiday[0]


    for i in range(1, len(holidays.DE(state=state, years=year))):
        hd = hd | mask_holiday[i]
    df['MO'] = df['Date'].apply(lambda x: x.weekday() == 0)
    df['MO'] = df['MO'] & (~hd)
    df['DI'] = df['Date'].apply(lambda x: x.weekday() == 1)
    df['DI'] = df['DI'] & (~hd)
    df['MI'] = df['Date'].apply(lambda x: x.weekday() == 2)
    df['MI'] = df['MI'] & (~hd)
    df['DO'] = df['Date'].apply(lambda x: x.weekday() == 3)
    df['DO'] = df['DO'] & (~hd)
    df['FR'] = df['Date'].apply(lambda x: x.weekday() == 4)
    df['FR'] = df['FR'] & (~hd)
    df['SA'] = df['Date'].apply(lambda x: x.weekday() == 5)
    df['SA'] = df['SA'] & (~hd)
    df['SO'] = df['Date'].apply(lambda x: x.weekday() == 6)
    df['SO'] = df['SO'] | hd
    hld = [(datetime.date(int(year), 12, 24)),
           (datetime.date(int(year), 12, 31))]
    
    mask = df['Day'].isin(hld)
    df.loc[mask, ['MO', 'DI', 'MI', 'DO', 'FR', 'SO']] = False
    df.loc[mask, 'SA'] = True


    par = pd.DataFrame.from_dict(gas_load_profile_parameters_dict())
    for slp in par.index:
        df['FW_'+str(slp)] = 0.0
        for wd in ['MO', 'DI', 'MI', 'DO', 'FR', 'SA', 'SO']:
            df.loc[df[wd], ['FW_'+str(slp)]] = par.loc[slp, wd]
    
    return_df = df.drop(columns=['DayOfYear']).set_index('Day')

    return return_df



def h_value(slp: str, regional_id_list: list, temperature_allocation: pd.DataFrame):
    """
    Returns h-values depending on allocation temperature  for every
    district.

    Args:
        slp : str
            Must be one of ['BA', 'BD', 'BH', 'GA', 'GB', 'HA',
                            'KO', 'MF', 'MK', 'PD', 'WA']
        regional_id_list : list of district keys in state e.g. ['11000'] for Berlin
        temperature_allocation : pd.DataFrame with results from allocation_temperature(year)
        

    Returns:
        pd.DataFrame
    """


    # filter temperature_df for the given districts
    temperature_df_districts = temperature_allocation.copy()[regional_id_list]

    par = gas_load_profile_parameters_dict()
    A = par['A'][slp]
    B = par['B'][slp]
    C = par['C'][slp]
    D = par['D'][slp]
    mH = par['mH'][slp]
    bH = par['bH'][slp]
    mW = par['mW'][slp]
    bW = par['bW'][slp]

    # calculate h-values for every district and every day
    all_dates_of_the_year = temperature_df_districts.index.to_numpy()
    for district in regional_id_list:
        for date in all_dates_of_the_year:
            temperature_df_districts.loc[date, district] = ((A / (1 + pow(B / (temperature_df_districts.loc[date, district] - 40), C)) + D)
                                     + max(mH * temperature_df_districts.loc[date, district] + bH, mW * temperature_df_districts.loc[date, district] + bW))
    
    return temperature_df_districts