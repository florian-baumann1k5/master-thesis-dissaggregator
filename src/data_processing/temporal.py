import logging
import pandas as pd
import holidays
import datetime

from src import logger


def shift_load_profile_generator(state, low=0.5, year=2015):
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
        pd.DataFrame
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
    periods = len(df)) # = number of 15min takts in the year
    


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
    


    
    for sp in ['S1_WT', 'S1_WT_SA', 'S1_WT_SA_SO', 'S2_WT',
               'S2_WT_SA', 'S2_WT_SA_SO', 'S3_WT', 'S3_WT_SA',
               'S3_WT_SA_SO']:
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