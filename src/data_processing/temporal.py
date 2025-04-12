import logging
import pandas as pd
import holidays
import datetime

from src import logger
from src.data_access.local_reader import *
from src.pipeline.pipe_applications import *

def disagg_temporal_industry(energy_carrier: str, year: int, low=0.5):
    """
    Disaggregate the consumption data by industry sector and shift load profiles.
    """

    # 0. validate input
    if year < 2000 or year > 2050:
        raise ValueError("Year must be between 2000 and 2050")
    if energy_carrier not in ["power", "gas"]:
        raise ValueError("Energy carrier must be either 'power' or 'gas'")
    

    #1. get consumption data for inustry sector
    consumption_data = disagg_applications_efficiency_factor(sector="industry", energy_carrier=energy_carrier, year=year)
    # 1.1 remove the application dissagg, but still has the eff factor -> df with 40 rows x 29 columns
    consumption_data = consumption_data.groupby(level=0, axis=1).sum()
    #1.2 add federal state to the consumption data in a new column "federal_state"
    consumption_data = consumption_data.assign(federal_state=lambda x: [federal_state_dict().get(int(i[: -3]))
                                       for i in x.index.astype(str)])


    #2. get shift load profiles
    slp = get_shift_load_profiles_by_year(year=year, low=low, force_preprocessing=False)
    slp.index = pd.to_datetime(slp.index)


    #3. create dataframe with index of all 15min steps in the year
    idx = pd.date_range(start=str(year), end=str(year+1), freq='15T')[:-1]
    DF = pd.DataFrame(index=idx)


    # 3.1 get all states
    states = federal_state_dict().values()


    # 4. iterate over all states
    for state in states:

        # 4.1 filter the consumption data for the state and add shift profiles
        # Filter for the current state
        sv_lk_wz = consumption_data.loc[consumption_data['federal_state'] == state]
        sv_lk_wz = sv_lk_wz.drop(columns=['federal_state'])
        # Fill missing values with 0
        sv_lk_wz = sv_lk_wz.fillna(0)
        sv_lk_wz = sv_lk_wz.transpose()
        # Assign load profiles based on industry index
        profiles = shift_profile_industry()  # cache function call
        sv_lk_wz['load_profile'] = [profiles[int(i)] for i in sv_lk_wz.index]


        # 4.2 get shift load profiles for the state
        sp_bl = slp.loc[:, slp.columns.get_level_values(0) == state]


        #4.3 Check the alignment of the time-indizes
        if not slp.index.equals(idx):
            mismatched = slp.index.symmetric_difference(idx)
            raise AssertionError(f"The time indexes are not aligned. Mismatched entries:\n{mismatched}. Could also be the type of the index!")


        sv_lk_wz_ts = pd.DataFrame(index=idx)


        # 4.4 iterate over all columns of the consumption data
        load_profiles = sv_lk_wz['load_profile'].unique()
        for load_profile in load_profiles:
            


        
        

    







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

