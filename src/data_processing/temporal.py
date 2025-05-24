import logging
import pandas as pd
import holidays
import datetime
from datetime import timedelta
from typing import Dict, Tuple
import time


from src import logger
from src.data_access.local_reader import *
from src.pipeline.pipe_applications import *
from src.utils.utils import *
from src.data_processing.temperature import *
from src.data_processing.consumption import *



# main functions
def disaggregate_temporal_industry(consumption_data: pd.DataFrame, year: int, low=0.5, force_preprocessing: bool = False) -> pd.DataFrame:
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
    slp = get_shift_load_profiles_by_year(year=year, low=low, force_preprocessing=force_preprocessing)
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


def disagg_temporal_heat_CTS(consumption_data: pd.DataFrame, year: int, state_list: list = federal_state_dict().values()) -> pd.DataFrame:
    """
    Disaggregates the temporal distribution of heat consumption for CTS in a given year.

    DISS 4.4.3.2 Erstellung von Wärmebedarfszeitreihen

    The consumpton for CTS of gas is highly dependent on the temperature since most of it is consumed for heating. 
    In this function we follow the approcha created by BDEW to disaggregate the gas consumption for CTS into hourly values.

    Args:
        consumption_data: DataFrame containing consumption data with columns ['regional_id', 'industry_sector']
        year: The year for the analysis


    Returns:
        pd.DataFrame:
            MultiIndex columns: [regional_id, industry_sector]
            index: hours of the given year
    """

    # 1. get the number of hours in the year
    hours_of_year = get_hours_of_year(year)


    # 2. get the temperature allocation for a future year per
    daily_temperature_allocation = allocation_temperature_by_day(year=year)


    # 3. create a empty dataframe with all regional ids and 15min steps
    df = pd.DataFrame(0, columns=daily_temperature_allocation.columns,
                    index=pd.date_range((str(year) + '-01-01'),
                                        periods=hours_of_year, freq='h'))

    # 4. iterate over all states
    for state in state_list:

        logger.info(f"Disaggregating gas consumption for state: {state}")

        tw_df = disagg_daily_gas_slp_cts(gas_consumption=consumption_data, state=state, temperatur_df=daily_temperature_allocation, year=year)

        # adds new column "BL" to gv_lk with the abbreviation of the state based on the regional code
        gv_lk = (consumption_data.assign(federal_state=[federal_state_dict().get(int(x[:-3]))
                                    for x in consumption_data.index.astype(str)]))

        # filter temperatur_df for the regional codes of the state and save it in t_allo_df
        daily_temperature_allocation.columns = daily_temperature_allocation.columns.astype(str)
        t_allo_df = daily_temperature_allocation[gv_lk.loc[gv_lk['federal_state'] == state].index.astype(str)]

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


        f_wd = ['FW_BA', 'FW_BD', 'FW_BH', 'FW_GA', 'FW_GB', 'FW_HA',
                'FW_KO', 'FW_MF', 'FW_MK', 'FW_PD', 'FW_WA',
                'FW_SpaceHeating-MFH', 'FW_SpaceHeating-EFH',
                'FW_Cooking_HotWater-HKO']
        calender_df = (gas_slp_weekday_params(state, year=year).drop(columns=f_wd))

        temp_calender_df = (pd.concat([calender_df.reset_index(), t_allo_df.reset_index()], axis=1))
        


        if temp_calender_df.isnull().values.any():
            raise KeyError('The chosen historical weather year and the '
                            'chosen projected year have mismatching '
                            'lengths. This could be due to gap years. '
                            'Please change the historical year in '
                            'hist_weather_year() in config.py to a year of '
                            'matching length.')

        # Create new column "Tagestyp" in temp_calender_df. Fill it with the weekday of the date based on the columns MO, DI, MI, DO, FR, SA, SO of the df
        temp_calender_df['Tagestyp'] = 'MO'
        for typ in ['DI', 'MI', 'DO', 'FR', 'SA', 'SO']:
            (temp_calender_df.loc[temp_calender_df[typ], 'Tagestyp']) = typ



        # create a list of all regional codes of the given state
        regional_id_list = gv_lk.loc[gv_lk['federal_state'] == state].index.astype(str)


        # iterate over every regional code in the list_lk... 'TODO: dauert
        for regional_id in regional_id_list:

            logger.info(f"Disaggregating gas consumption for regional id: {regional_id} in state: {state}")
             # create empty df with index equal to every hour of the year in the format e.g. 2018-01-04 18:00:00 starting from 2018-01-01 00:00:00
            lk_df = pd.DataFrame(index=pd.date_range((str(year) + '-01-01'), periods=hours_of_year, freq='h'))
            
            
            #tw_df_lk = tw_df.loc[int(regional_id),]
            regional_id_int = int(regional_id)

            # Get first level of column MultiIndex and convert to int
            col_level_0 = tw_df.columns.get_level_values(0).astype(int)

            # Filter columns safely
            tw_df_lk = tw_df.loc[:, col_level_0 == regional_id_int]
            #tw_df_lk = tw_df.loc[:, tw_df.columns.get_level_values(0) == int(regional_id)]
            tw_df_lk.columns = tw_df_lk.columns.get_level_values(1)
            tw_df_lk.columns = tw_df_lk.columns.astype(int)

            tw_df_lk.index = pd.DatetimeIndex(tw_df_lk.index)
            last_hour = tw_df_lk.copy()[-1:]
            last_hour.index = last_hour.index + timedelta(1)

             # add the first day of the year year+1 to the tw_df_lk
            tw_df_lk = pd.concat([tw_df_lk, last_hour])

            # add the hours to the tw_df_lk and remove the last hour -> got hours for the whole year: 2018-01-01 00:00:00 to 2018-12-31 23:00:00
            # Values for every hour of a day are the same
            tw_df_lk = tw_df_lk.resample('h').ffill()
            tw_df_lk = tw_df_lk[:-1]

            # get from temp_calender_df for every day the Tagestyp=Wochentag and the coulumn of the regional code we are currently iterating over
            temp_cal = temp_calender_df.copy()
            temp_cal = temp_cal[['Date', 'Tagestyp', regional_id]].set_index("Date")

            last_hour = temp_cal.copy()[-1:]
            last_hour.index = last_hour.index + timedelta(1)

            temp_cal = pd.concat([temp_cal, last_hour])

            #temp_cal.index = pd.to_datetime(temp_cal.index)
            temp_cal = temp_cal.resample('h').ffill()
            temp_cal = temp_cal[:-1]
            temp_cal['Stunde'] = pd.DatetimeIndex(temp_cal.index).time
            temp_cal = temp_cal.set_index(["Tagestyp", regional_id, 'Stunde'])

            # iterate over all load profiles/ industry_sectors
            for slp in list(dict.fromkeys(load_profiles_cts_gas().values())):


                slp_profil = load_gas_load_profile(slp)


                slp_profil = pd.DataFrame(slp_profil.set_index(['Tagestyp', 'Temperatur\nin °C\nkleiner']))
                slp_profil.columns = pd.to_datetime(slp_profil.columns, format='%H:%M:%S')
                slp_profil.columns = pd.DatetimeIndex(
                    slp_profil.columns).time
                slp_profil = slp_profil.stack()

                # First, compute the 'Prozent' column
                temp_cal['Prozent'] = [slp_profil[x] for x in temp_cal.index]

                # Prepare a dictionary to store the new columns
                new_cols = {}
                for wz in [k for k, v in load_profiles_cts_gas().items() if v.startswith(slp)]:
                    colname = f"{regional_id}_{wz}"
                    new_cols[colname] = tw_df_lk[wz].values * temp_cal['Prozent'].values / 100

                # Convert the dictionary to a DataFrame using the same index as lk_df (and df)
                new_cols_df = pd.DataFrame(new_cols, index=lk_df.index)

                # Concatenate the new columns to your existing DataFrames at once
                lk_df = pd.concat([lk_df, new_cols_df], axis=1)
                df = pd.concat([df, new_cols_df], axis=1)
            df[str(regional_id)] = lk_df.sum(axis=1)

    # 5. drop the regional id consumption columns
    df.columns = df.columns.astype(str)
    df = df.drop(columns=gv_lk.index.astype(str))

    # 6. make the columns a multiindex
    df.columns = pd.MultiIndex.from_tuples([(int(x), int(y)) for x, y in
                                    df.columns.str.split('_')])
    

    # sanity check
    if df.isna().any().any():
        raise ValueError(f"The disaggregated temporal consumption contains NaN values in year {year}")
    
    return df


def disaggregate_temporal_power_CTS(consumption_data: pd.DataFrame, year: int) -> pd.DataFrame: 
    """
    This is the old function
    
    """
    
    sv_yearly = consumption_data.assign(BL=lambda x: [federal_state_dict().get(int(i[: -3]))
                                        for i in x.index.astype(str)])
    

    total_sum = sv_yearly.drop('BL', axis=1).sum().sum()

    # Create empty 15min-index'ed DataFrame for target year
    idx = pd.date_range(start=str(year), end=str(year+1), freq='15T')[:-1]
    DF = pd.DataFrame(index=idx)

    for state in federal_state_dict().values():
        logger.info('Working on state: {}.'.format(state))
        sv_lk_wz = (sv_yearly
                    .loc[lambda x: x['BL'] == state]
                    .drop(columns=['BL'])
                    .transpose()
                    .assign(SLP=lambda x: [load_profiles_cts_power()[int(i)] for i in x.index]))

        logger.info('... creating state-specific load-profiles')
        slp_bl = get_CTS_power_slp(state, year=year)
        # Plausibility check:
        assert slp_bl.index.equals(idx), "The time-indizes are not aligned"
        # Create 15min-index'ed DataFrames for current state

        sv_lk_wz_ts = pd.DataFrame(index=idx)


        logger.info('... assigning load-profiles to WZs')
        for slp in sv_lk_wz['SLP'].unique():
            sv_lk = (sv_lk_wz.loc[sv_lk_wz['SLP'] == slp].drop(columns=['SLP']).stack().reset_index())

            # renaming column if neccessary
            sv_lk.columns = ["regional_id" if col == "level_1" else col for col in sv_lk.columns]

            sv_lk = (sv_lk.assign(LK_WZ=lambda x: x.regional_id.astype(str) + '_' + x.industry_sector.astype(str))
                     .set_index('LK_WZ')
                     .drop(['industry_sector', 'regional_id'], axis=1)
                     .loc[lambda x: x[0] >= 0]
                     .transpose())

            # Calculate load profile for each LK and WZ
            lp_lk_wz = (pd.DataFrame(np.multiply(slp_bl[[slp]].values,
                                                     sv_lk.values),
                                         index=slp_bl.index,
                                         columns=sv_lk.columns))

            # Merge intermediate results
            sv_lk_wz_ts = (sv_lk_wz_ts.merge(lp_lk_wz, left_index=True,
                                                 right_index=True,
                                                 suffixes=(False, False)))


        # Concatenate the state-wise results
         # restore MultiIndex as integer tuples
        sv_lk_wz_ts.columns =\
                pd.MultiIndex.from_tuples([(int(x), int(y)) for x, y in
                                           sv_lk_wz_ts.columns.str.split('_')])
        
        DF = pd.concat([DF, sv_lk_wz_ts], axis=1)
        DF.columns = pd.MultiIndex.from_tuples(DF.columns, names=['LK', 'WZ'])


    # Plausibility check:
    msg = ('The sum of yearly consumptions (={:.3f}) and the sum of disaggrega'
           'ted consumptions (={:.3f}) do not match! Please check algorithm!')
    disagg_sum = DF.sum().sum()
    assert np.isclose(total_sum, disagg_sum), msg.format(total_sum, disagg_sum)

    
    return DF


def disagg_temporal_petrol_CTS(consumption_data: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Disaggregate the consumption data for petrol in the CTS sector like gas
    """

    df = disagg_temporal_heat_CTS(consumption_data=consumption_data, year=year)

    # sanity check
    if not np.isclose(df.sum().sum(), consumption_data.sum().sum(), atol=1e-6):
        raise ValueError(f"The sum of the disaggregated temporal consumption is not equal to the sum of the initial consumption data in year {year}")
    if df.isna().any().any():
        raise ValueError(f"The disaggregated temporal consumption contains NaN values in year {year}")

    return df





# utils

def get_shift_load_profiles_by_state_and_year(state: str, low: float = 0.5, year: int = 2015): 

    """
    Return shift load profiles in normalized units
    ('normalized' means that the sum over all time steps equals to one).

    DISS 4.4.1
    old function: shift_load_profile_generator()

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
        Index:
        Columns: 
            unrelevant: ['Day', 'Hour', 'DayOfYear', 'WD', 'SA', 'SU', 'WIZ', 'SOZ', 'UEZ']
            die SLPs: ['H0', 'L0', 'L1', 'L2', 'G0', 'G1', 'G2', 'G3', 'G4', 'G5', 'G6']
        -> the sum of the SLP columns equals ~1
    """
    def Leistung(Tag_Zeit, mask, df, df_SLP):
        u = pd.merge(df[mask], df_SLP[['Hour', Tag_Zeit]], on=['Hour'], how='left')
        v = pd.merge(df, u[['Date', Tag_Zeit]], on=['Date'], how='left')
        v_filled = v.infer_objects(copy=False).fillna(0.0)
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
         combined_slp = load_shift_load_profiles_by_year_cache(year=year)

         if combined_slp is not None:
            return combined_slp
    
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

    return combined_slp


def disagg_daily_gas_slp_cts(gas_consumption: pd.DataFrame, state: str, temperatur_df: pd.DataFrame, year: int, force_preprocessing: bool = False):
    """
    Disaggregates the daily gas consumption for CTS in a given state and year.

    Args:
        state: str
        temperatur_df: pd.DataFrame
        gas_consumption: pd.DataFrame: gas consumption data for every industry sector (columns) and regional_id (index)
    
    Returns:
        pd.DataFrame:
            MultiIndex columns: [regional_id, industry_sector]
            index: days of the year
    """


    # 0. get the number of days in the year
    days_of_year = get_days_of_year(year)


    # 1. transform gas consumption
    gv_lk = gas_consumption.copy()
    # add Bundesland column to gv_lk (removeing last 3 digits of region_code and doing lookup in federal_state_dict() to get Bundesland)
    gv_lk = (gv_lk.assign( federal_state =[federal_state_dict().get(int(x[: -3]))
                          for x in gv_lk.index.astype(str)]))
    
    df = pd.DataFrame(index=range(days_of_year))
    #filter for the state in the function arguments and transpose df
    gv_lk = gv_lk.loc[gv_lk['federal_state'] == state].drop(columns=['federal_state']).transpose()


    # 2. add SLP column based on industry sectors (see mapping load_profiles_cts_gas())
    list_ags = gv_lk.columns.astype(str)
    gv_lk.index = gv_lk.index.astype('int64')
    gv_lk['SLP'] = [load_profiles_cts_gas()[x] for x in (gv_lk.index)]


    # 1. get weekday-parameters of the gas standard load profiles
    F_wd = (gas_slp_weekday_params(state=state, year=year)
            .drop(columns=['MO', 'DI', 'MI', 'DO', 'FR', 'SA', 'SO'])
            .set_index('Date'))


    # 
    tageswerte = pd.DataFrame(index=F_wd.index)
    logger.info('... creating state-specific load-profiles')
    

    # x. iterate over the unique SLPs
    for slp in gv_lk['SLP'].unique():


        F_wd_slp = F_wd[['FW_'+slp]]
        h_slp = h_value(slp, list_ags, temperatur_df)



        if (len(h_slp) != len(F_wd_slp)):
            raise KeyError('The chosen historical weather year and the chosen '
                           'projected year have mismatching lengths.'
                           'This could be due to gap years. Please change the '
                           'historical year in hist_weather_year() in '
                           'mappings.py to a year of matching length.')

        tw = pd.DataFrame(np.multiply(h_slp.values, F_wd_slp.values),
                          index=h_slp.index, columns=h_slp.columns)
        
        
        tw_norm = tw/tw.sum()
        tw_norm.columns = tw_norm.columns.astype(str)

        gv_df = (gv_lk.loc[gv_lk['SLP'] == slp].drop(columns=['SLP'])
                      .stack().reset_index())
        tw_lk_wz = pd.DataFrame(index=h_slp.index)


        for lk in gv_df['regional_id'].unique():
            gv_slp = (gv_df.loc[gv_df['regional_id'] == lk]
                           .drop(columns=['regional_id'])
                           .set_index('industry_sector').transpose()
                           .rename(columns=lambda x: str(lk) + '_' + str(x)))
            tw_lk_wz_slp = (pd.DataFrame(np.multiply(tw_norm[
                                                     [str(lk)]
                                                     * len(gv_slp.columns)]
                                                     .values, gv_slp.values),
                                         index=tw_norm.index,
                                         columns=gv_slp.columns))
            tw_lk_wz = pd.concat([tw_lk_wz, tw_lk_wz_slp], axis=1)
        tw_lk_wz.index = pd.to_datetime(tw_lk_wz.index)
        tw_lk_wz.index.name = 'Date'
        tageswerte = pd.concat([tageswerte, tw_lk_wz], axis=1)



    tageswerte = tageswerte.dropna(how='all')
    df = tageswerte.iloc[:days_of_year]
    

    def safe_split(col):
        parts = col.split('_')
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            return (int(parts[0]), int(parts[1]))
        else:
            raise ValueError(f"Invalid column format: {col}")

    df.columns = pd.MultiIndex.from_tuples([safe_split(col) for col in df.columns], names=["regional_id", "industry_sector"])
    

    # sanity check that df is not empty or only contains 0.0
    if df.isna().any().any():
        raise ValueError("DataFrame contains NaN values")
    if df.empty:
        raise ValueError("DataFrame is empty")
    if df.sum().sum() == 0.0:
        raise ValueError("DataFrame only contains 0.0")
    if df.shape[0] != days_of_year:
        raise ValueError(f"DataFrame has {df.shape[0]} rows, but should have {days_of_year}")

    



    return df

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
    Returns h-values depending on allocation temperature for every district.

    DISS S.80f.

    Args:
        slp : str
            Must be one of ['BA', 'BD', 'BH', 'GA', 'GB', 'HA',
                            'KO', 'MF', 'MK', 'PD', 'WA']
        regional_id_list : list of district keys in state e.g. ['11000'] for Berlin
        temperature_allocation : pd.DataFrame with results from allocation_temperature_by_day(year)
        

    Returns:
        pd.DataFrame
    """

    logger.info(f" calculateing h_value for slp: {slp} and regional_ids: {regional_id_list} ")


    # filter temperature_df for the given districts
    temperature_allocation.columns = temperature_allocation.columns.astype(int)
    regional_id_list = [int(rid) for rid in regional_id_list]
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
        logger.info(f"calculateing h_value for district: {district} ")
        for date in all_dates_of_the_year:
            temperature_df_districts.loc[date, district] = ((A / (1 + pow(B / (temperature_df_districts.loc[date, district] - 40), C)) + D)
                                     + max(mH * temperature_df_districts.loc[date, district] + bH, mW * temperature_df_districts.loc[date, district] + bW))
    
    return temperature_df_districts



def disagg_temporal_heat_CTS_water_by_state(state: str, year: int, energy_carrier: str):
    """
    Disagreggate spatial data of CTS' gas demand temporally.

    detailed : bool, default False
        If True return 'per district and branch' else only 'per district'
    use_nuts3code : bool, default False
        If True use NUTS-3 codes as region identifiers.
    state : str, default None
        Specifies state. Must by one of the entries of bl_dict().values(),
        ['SH', 'HH', 'NI', 'HB', 'NW', 'HE', 'RP', 'BW', 'BY', 'SL', 'BE',
         'BB', 'MV', 'SN', 'ST', 'TH']
    
         
    Returns:
        pd.DataFrame
        index: hours of the given year
        columns: MultiIndex(levels=[regional_id, industry_sector])
    """
    assert state in list(federal_state_dict().values()), ("'state' needs to be in ['SH',"
                                               "'HH', 'NI', 'HB', 'NW', 'HE',"
                                               "'RP', 'BW', 'BY', 'SL', 'BE',"
                                               "'BB', 'MV', 'SN', 'ST', 'TH']")
    assert isinstance(state, str), "'state' needs to be a string."



    # 1. get the number of hours in the year
    hours_of_year = get_hours_of_year(year)


    # 2. get the temperature allocation
    daily_temperature_allocation = allocation_temperature_by_day(year=year)
    daily_temperature_allocation.columns = daily_temperature_allocation.columns.astype(str)


    # Below 15°C the water heating demand is assumed to be constant
    daily_temperature_allocation.clip(15, inplace=True)


    # create DataFrame from temperature and use timestamp as index
    df = pd.DataFrame(0, columns=daily_temperature_allocation.columns,
                        index=pd.date_range((str(year) + '-01-01'), periods=hours_of_year, freq='H'))
    

    # for state in bl_dict().values():
    logger.info(f'Working on state: {state}.')
    tw_df, gv_lk = disagg_daily_gas_slp_water(state, daily_temperature_allocation, year=year, energy_carrier=energy_carrier)
    
    gv_lk = (gv_lk.assign(federal_state=[federal_state_dict().get(int(x[:-3]))
                                for x in gv_lk.index.astype(str)]))
    

    daily_temperature_allocation.columns = daily_temperature_allocation.columns.map(str)
    regional_ids = gv_lk.loc[gv_lk['federal_state'] == state].index.astype(str).tolist()
    t_allo_df = daily_temperature_allocation[regional_ids]

    t_allo_df.values[:] = 100  # changed
    t_allo_df = t_allo_df.astype('int32')

    f_wd = ['FW_BA', 'FW_BD', 'FW_BH', 'FW_GA', 'FW_GB', 'FW_HA', 'FW_KO',
            'FW_MF', 'FW_MK', 'FW_PD', 'FW_WA']
    

    calender_df = (gas_slp_weekday_params(state, year=year).drop(columns=f_wd))

    temp_calender_df = (pd.concat([calender_df.reset_index(), t_allo_df.reset_index()], axis=1))

    if temp_calender_df.isnull().values.any():
        raise KeyError('The chosen historical weather year and the chosen '
                        'projected year have mismatching lengths.'
                        'This could be due to gap years. Please change the '
                        'historical year in hist_weather_year() in '
                        'config.py to a year of matching length.')

    temp_calender_df['Tagestyp'] = 'MO'
    for typ in ['DI', 'MI', 'DO', 'FR', 'SA', 'SO']:
        (temp_calender_df.loc[temp_calender_df[typ], 'Tagestyp']) = typ


    # create a list of all regional codes of the given state
    regional_id_list = gv_lk.loc[gv_lk['federal_state'] == state].index.astype(str)

    # iterate over all regional codes
    for regional_id in regional_id_list:
        lk_df = pd.DataFrame(index=pd.date_range((str(year) + '-01-01'), periods=hours_of_year, freq='H'))
        tw_df_lk = tw_df.loc[:, int(regional_id)]
        tw_df_lk.index = pd.DatetimeIndex(tw_df_lk.index)
        last_hour = tw_df_lk.copy()[-1:]
        last_hour.index = last_hour.index + timedelta(1)



            # add the first day of the year year+1 to the tw_df_lk
        tw_df_lk = pd.concat([tw_df_lk, last_hour])


        # add the hours to the tw_df_lk and remove the last hour -> got hours for the whole year: 2018-01-01 00:00:00 to 2018-12-31 23:00:00
        # Values for every hour of a day are the same
        tw_df_lk = tw_df_lk.resample('h').ffill()
        tw_df_lk = tw_df_lk[:-1]



        # get from temp_calender_df for every day the Tagestyp=Wochentag and the coulumn of the regional code we are currently iterating over
        temp_cal = temp_calender_df.copy()
        temp_cal = temp_cal[['Date', 'Tagestyp', regional_id]].set_index("Date")


        last_hour = temp_cal.copy()[-1:]
        last_hour.index = last_hour.index + timedelta(1)


        temp_cal = pd.concat([temp_cal, last_hour])



        #temp_cal.index = pd.to_datetime(temp_cal.index)
        temp_cal = temp_cal.resample('h').ffill()

        temp_cal = temp_cal[:-1]
        temp_cal['Stunde'] = pd.DatetimeIndex(temp_cal.index).time
        temp_cal = temp_cal.set_index(["Tagestyp", regional_id, 'Stunde'])

        for slp in list(dict.fromkeys(load_profiles_cts_gas().values())):

            slp_profil = load_gas_load_profile(slp)
            
            slp_profil = pd.DataFrame(slp_profil.set_index(
                ['Tagestyp', 'Temperatur\nin °C\nkleiner']))
            slp_profil.columns = pd.to_datetime(slp_profil.columns,
                                                format='%H:%M:%S')
            slp_profil.columns = pd.DatetimeIndex(slp_profil.columns).time
            slp_profil = slp_profil.stack()
            temp_cal['Prozent'] = [slp_profil[x] for x in temp_cal.index]
            for wz in [k for k, v in load_profiles_cts_gas().items() if v.startswith(slp)]:
                lk_df[str(regional_id) + '_' + str(wz)] = (tw_df_lk[wz].values
                                                    * temp_cal['Prozent'].values/100)
                
                df[str(regional_id) + '_' + str(wz)] = (tw_df_lk[wz].values
                                                * temp_cal['Prozent'].values/100)



        df[str(regional_id)] = lk_df.sum(axis=1)
 


    df = df.drop(columns=gv_lk.index.astype(str))
    df.columns = pd.MultiIndex.from_tuples([(int(x), int(y)) for x, y in
                                    df.columns.str.split('_')])
    
    # sanity check
    if df.isna().any().any():
        raise ValueError("DataFrame contains NaN values")
    if df.empty:
        raise ValueError("DataFrame is empty")
    if df.sum().sum() == 0.0:
        raise ValueError("DataFrame only contains 0.0")

    return df



def disagg_daily_gas_slp_water(state: str, temperatur_df: pd.DataFrame, year: int, energy_carrier: str):
    """
    Returns daily demand of gas with a given yearly demand in MWh
    per district and SLP.

    state: str
        must be one of ['BW','BY','BE','BB','HB','HH','HE','MV',
                        'NI','NW','RP','SL','SN','ST','SH','TH']
    Returns
    -------
    pd.DataFrame
    """
    
    # 1. get the number of days in the year
    days_of_year = get_days_of_year(year)


    # 2. filter consumption
    # returns:
    #   index: regional_id
    #   columns: industry_sectors
    #   values: consumption of ['hot_water', 'mechanical_energy', 'process_heat'] per industry_sector and regional_id
    df_eff = disagg_applications_efficiency_factor(energy_carrier=energy_carrier, sector="cts", year=year)
    df_eff_reordered = df_eff.reorder_levels(order=[1, 0], axis=1)
    df_eff_selected = df_eff_reordered.loc[:, ['hot_water', 'mechanical_energy', 'process_heat']]
    gv_lk = df_eff_selected.groupby(level=1, axis=1).sum()
    


    gv_lk.columns.name = None
    gv_lk_return = gv_lk.copy()  # save for later return
    gv_lk = (gv_lk.assign(federal_state=[federal_state_dict().get(int(x[: -3]))
                          for x in gv_lk.index.astype(str)]))
    

    df = pd.DataFrame(index=range(days_of_year))

    gv_lk = gv_lk.loc[gv_lk['federal_state'] == state].drop(columns=['federal_state']).transpose()

    list_ags = gv_lk.columns.astype(str)

    gv_lk['default_load_profile'] = [load_profiles_cts_gas()[int(x)] for x in (gv_lk.index)]
    F_wd = (gas_slp_weekday_params(state, year=year)
            .drop(columns=['MO', 'DI', 'MI', 'DO', 'FR', 'SA', 'SO'])
            .set_index('Date'))
    

    tageswerte = pd.DataFrame(index=F_wd.index)

    # 3. iterate over all load profiles
    all_slps = gv_lk['default_load_profile'].unique()
    for slp in all_slps:
        F_wd_slp = F_wd[['FW_'+slp]]
        h_slp = h_value_water(slp, list_ags, temperatur_df)

        if (len(h_slp) != len(F_wd_slp)):
            raise KeyError('The chosen historical weather year and the chosen '
                           'projected year have mismatching lengths.'
                           'This could be due to gap years. Please change the '
                           'historical year in hist_weather_year() in '
                           'config.py to a year of matching length.')

        tw = pd.DataFrame(np.multiply(h_slp.values, F_wd_slp.values),
                          index=h_slp.index, columns=h_slp.columns)
        tw_norm = tw / tw.sum()
        tw_norm.columns = tw_norm.columns.astype(str)
        gv_df = (gv_lk.loc[gv_lk['default_load_profile'] == slp].drop(columns=['default_load_profile'])
                      .stack().reset_index())
        tw_lk_wz = pd.DataFrame(index=h_slp.index)


        for lk in gv_df['regional_id'].unique():
            gv_slp = (gv_df.loc[gv_df['regional_id'] == lk]
                           .drop(columns=['regional_id'])
                           .set_index('level_0').transpose()
                           .rename(columns=lambda x: str(lk) + '_' + str(x)))
            tw_lk_wz_slp = (pd.DataFrame(np.multiply(tw_norm[[str(lk)]
                                                     * len(gv_slp.columns)]
                                                     .values, gv_slp.values),
                                         index=tw_norm.index,
                                         columns=gv_slp.columns))
            tw_lk_wz = pd.concat([tw_lk_wz, tw_lk_wz_slp], axis=1)
        tageswerte = pd.concat([tageswerte, tw_lk_wz], axis=1)


    df = tageswerte.iloc[-days_of_year:]


    df.columns = pd.MultiIndex.from_tuples([(int(x), int(y)) for x, y in
                                   df.columns.str.split('_')])
    

    return [df, gv_lk_return]


def h_value_water(slp, regional_id_list, temperatur_df):
    """
    Returns h-values depending on allocation temperature for every district.

    Parameter
    -------
    slp : str
        Must be one of ['BA', 'BD', 'BH', 'GA', 'GB', 'HA',
                        'KO', 'MF', 'MK', 'PD', 'WA']
    districts : list of district keys in state e.g. ['11000'] for Berlin

    Returns
    -------
    pd.DataFrame
    """
    logger.info(f"calculateing h_value_water for slp: {slp} and regional_ids: {regional_id_list} ")
    
    temperatur_df.columns = temperatur_df.columns.astype(int)
    regional_id_list = [int(rid) for rid in regional_id_list]
    temp_df = temperatur_df.copy()[[x for x in regional_id_list]]


    # Below 13 °C, the water heating demand is not defined and assumed to stay constant
    temp_constant = 13
    temp_df.clip(temp_constant, inplace=True)


    par = gas_load_profile_parameters_dict()
    D = par['D'][slp]
    mW = par['mW'][slp]
    bW = par['bW'][slp]

    for regional_id in regional_id_list:
        # Vectorized assignment to update the entire column
        temp_df[regional_id] = D + mW * temp_df[regional_id] + bW
    return temp_df


