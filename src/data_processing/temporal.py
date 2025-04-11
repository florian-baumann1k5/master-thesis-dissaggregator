import logging
import pandas as pd
import holidays
import datetime

from src import logger
from src.data_access.local_reader import *

def disagg_temporal_industry(energy_carrier: str, year: int, low=0.5):
    """
    Disaggregate the consumption data by industry sector and shift load profiles.
    """



    return None


def get_shift_load_profiles(state, low=0.5, year=2015): 

    """
    Return shift load profiles in normalized units
    ('normalized' means that the sum over all time steps equals to one).

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
    # TODO Flo: Description "Default is set to 0.35" but default is 0.5 in the argument list


    # 0. validate input
    if state not in ['BW', 'BY', 'BE', 'BB', 'HB', 'HH', 'HE', 'MV',
                        'NI', 'NW', 'RP', 'SL', 'SN', 'ST', 'SH', 'TH']:
        raise ValueError(f"Invalid state: {state}")

    # 1. Create datetime index for the full year in 15-minute steps
    idx = pd.date_range(start=f'{year}-01-01', end=f'{year+1}-01-01', freq='15min', closed='left')
    # Build DataFrame and extract features using .dt accessors (faster + cleaner)
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
    


    
    for sp in ['S1_WT', 'S1_WT_SA', 'S1_WT_SA_SO', 'S2_WT', 'S2_WT_SA', 'S2_WT_SA_SO', 'S3_WT', 'S3_WT_SA', 'S3_WT_SA_SO']:
        if(sp == 'S1_WT'):
            anzahl_wz = 17 / 48 * len(df[df['WT']])
            anzahl_nwz = (31 / 48 * len(df[df['WT']]) + len(df[df['SO']]) + len(df[df['SA']]))
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = (df['SO'] | df['SA'])
            df.loc[mask, sp] = low * anteil
            mask = ((df['WT']) & ((df['Hour'] < pd.to_datetime('08:00:00').time()) | (df['Hour'] >= pd.to_datetime('16:30:00').time())))
            df.loc[mask, sp] = low * anteil
    
        elif(sp == 'S1_WT_SA'):
            anzahl_wz = (17 / 48 * len(df[df['WT']]) + 17 / 48 * len(df[df['SA']]))
            anzahl_nwz = (31 / 48 * len(df[df['WT']]) + len(df[df['SO']]) + 31/48 * len(df[df['SA']]))
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = df['SO']
            df.loc[mask, sp] = low * anteil
            mask = ((df['WT']) & ((df['Hour'] < pd.to_datetime('08:00:00').time()) | (df['Hour'] >= pd.to_datetime('16:30:00').time())))
            df.loc[mask, sp] = low * anteil
            mask = ((df['SA']) & ((df['Hour'] < pd.to_datetime('08:00:00').time()) | (df['Hour'] >= pd.to_datetime('16:30:00').time())))
            df.loc[mask, sp] = low * anteil
        
        elif(sp == 'S1_WT_SA_SO'):
            anzahl_wz = (17 / 48 * (len(df[df['WT']]) + len(df[df['SO']]) + len(df[df['SA']])))
            anzahl_nwz = (31 / 48 * (len(df[df['WT']]) + len(df[df['SO']]) + len(df[df['SA']])))
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = ((df['Hour'] < pd.to_datetime('08:00:00').time()) | (df['Hour'] >= pd.to_datetime('16:30:00').time()))
            df.loc[mask, sp] = low * anteil
        
        elif(sp == 'S2_WT'):
            anzahl_wz = 17/24 * len(df[df['WT']])
            anzahl_nwz = (7/24 * len(df[df['WT']]) + len(df[df['SO']]) + len(df[df['SA']]))
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = (df['SO'] | df['SA'])
            df.loc[mask, sp] = low * anteil
            mask = ((df['WT']) & ((df['Hour'] < pd.to_datetime('06:00:00').time()) | (df['Hour'] >= pd.to_datetime('23:00:00').time())))
            df.loc[mask, sp] = low * anteil
        
        elif(sp == 'S2_WT_SA'):
            anzahl_wz = 17/24 * (len(df[df['WT']]) + len(df[df['SA']]))
            anzahl_nwz = (7/24 * len(df[df['WT']]) + len(df[df['SO']]) + 7/24 * len(df[df['SA']]))
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = df['SO']
            df.loc[mask, sp] = low * anteil
            mask = (((df['WT']) | (df['SA'])) & ((df['Hour'] < pd.to_datetime('06:00:00').time()) | (df['Hour'] >= pd.to_datetime('23:00:00').time())))
            df.loc[mask, sp] = low * anteil
        
        elif(sp == 'S2_WT_SA_SO'):
            anzahl_wz = (17/24 * (len(df[df['WT']]) + len(df[df['SA']]) + len(df[df['SO']])))
            anzahl_nwz = (7/24 * (len(df[df['WT']]) + len(df[df['SO']]) + len(df[df['SA']])))
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = (((df['Hour'] < pd.to_datetime('06:00:00').time()) | (df['Hour'] >= pd.to_datetime('23:00:00').time())))
            df.loc[mask, sp] = low * anteil
        
        elif(sp == 'S3_WT_SA_SO'):
            anteil = 1 / periods
            df[sp] = anteil
        
        elif(sp == 'S3_WT'):
            anzahl_wz = len(df[df['WT']])
            anzahl_nwz = len(df[df['SO']]) + len(df[df['SA']])
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = (df['SO'] | df['SA'])
            df.loc[mask, sp] = low * anteil
        
        elif(sp == 'S3_WT_SA'):
            anzahl_wz = len(df[df['WT']]) + len(df[df['SA']])
            anzahl_nwz = len(df[df['SO']])
            anteil = 1 / (anzahl_wz + low * anzahl_nwz)
            df[sp] = anteil
            mask = df['SO']
            df.loc[mask, sp] = low * anteil
    
    
    df = (df[['Date', 'S1_WT', 'S1_WT_SA', 'S1_WT_SA_SO', 'S2_WT', 'S2_WT_SA', 'S2_WT_SA_SO', 'S3_WT', 'S3_WT_SA', 'S3_WT_SA_SO']]
          .set_index('Date'))
    return df


def get_CTS_power_slp(state, year: int):
    """
    Return the electric standard load profiles in normalized units
    ('normalized' means here that the sum over all time steps equals one).

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


