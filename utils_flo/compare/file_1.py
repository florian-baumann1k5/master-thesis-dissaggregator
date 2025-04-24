def disagg_temporal_gas_CTS_water_by_state(detailed=False, use_nuts3code=False,
                                           state=None, **kwargs):
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
    Returns
    -------
    pd.DataFrame
    """
    assert state in list(bl_dict().values()), ("'state' needs to be in ['SH',"
                                               "'HH', 'NI', 'HB', 'NW', 'HE',"
                                               "'RP', 'BW', 'BY', 'SL', 'BE',"
                                               "'BB', 'MV', 'SN', 'ST', 'TH']")
    assert isinstance(state, str), "'state' needs to be a string."
    # get config and year, if not given in kwargs
    cfg = kwargs.get('cfg', get_config())
    year = kwargs.get('year', cfg['base_year'])
    # check if timeseries exits on hard drive
    if not detailed:
        path = data_out('CTS_gas_tempinde_' + str(state) + '_' + str(year)
                        + '.csv')
    else:
        path = data_out('CTS_gas_tempinde_detailed_' + str(state) + '_'
                        + str(year) + '.csv')
    if os.path.exists(path):
        logger.info('Reading temperature independent gas demand'
                    ' timeseries from hard drive.')
        if not detailed:
            gas_total = pd.read_csv(path, index_col=0)
            gas_total.index = pd.to_datetime(gas_total.index)
            gas_total.columns = gas_total.columns.astype(int)
        else:
            gas_total = pd.read_csv(path, header=[0, 1], index_col=0)
            gas_total.index = pd.to_datetime(gas_total.index)
            gas_total.columns = gas_total.columns.set_levels(
                [gas_total.columns.levels[0].astype(int),
                 gas_total.columns.levels[1].astype(int)])
        if use_nuts3code:
            gas_total = gas_total.rename(columns=dict_region_code(
                level='lk', keys='ags_lk', values='natcode_nuts3'),
                level=(0 if detailed else None))
        return gas_total
    # if does not exist on hard drive, calculate
    else:
        # check if gap year
        if ((year % 4 == 0) & (year % 100 != 0) | (year % 4 == 0)
                & (year % 100 == 0) & (year % 400 == 0)):
            hours = 8784
        else:
            hours = 8760
        temperatur_df = t_allo(year=year)  # get daily allocation temperature
        # Below 15°C the water heating demand is assumed to be constant
        temperatur_df.clip(15, inplace=True)
        # create DataFrame from temperature and use timestamp as index
        df = pd.DataFrame(0, columns=temperatur_df.columns,
                          index=pd.date_range((str(year) + '-01-01'),
                                              periods=hours, freq='H'))
        # for state in bl_dict().values():
        logger.info('Working on state: {}.'.format(state))
        tw_df, gv_lk = disagg_daily_gas_slp_water(state, temperatur_df,
                                                  year=year)
        gv_lk = (gv_lk.assign(BL=[bl_dict().get(int(x[:-3]))
                                  for x in gv_lk.index.astype(str)]))
        t_allo_df = temperatur_df[gv_lk.loc[gv_lk['BL'] == state]
                                       .index.astype(str)]

        t_allo_df.values[:] = 100  # changed
        t_allo_df = t_allo_df.astype('int32')

        f_wd = ['FW_BA', 'FW_BD', 'FW_BH', 'FW_GA', 'FW_GB', 'FW_HA', 'FW_KO',
                'FW_MF', 'FW_MK', 'FW_PD', 'FW_WA']
        calender_df = (gas_slp_weekday_params(state, year=year)
                       .drop(columns=f_wd))
        temp_calender_df = (pd.concat([calender_df.reset_index(),
                                       t_allo_df.reset_index()], axis=1))

        if temp_calender_df.isnull().values.any():
            raise KeyError('The chosen historical weather year and the chosen '
                           'projected year have mismatching lengths.'
                           'This could be due to gap years. Please change the '
                           'historical year in hist_weather_year() in '
                           'config.py to a year of matching length.')

        temp_calender_df['Tagestyp'] = 'MO'
        for typ in ['DI', 'MI', 'DO', 'FR', 'SA', 'SO']:
            (temp_calender_df.loc[temp_calender_df[typ], 'Tagestyp']) = typ
        list_lk = gv_lk.loc[gv_lk['BL'] == state].index.astype(str)
        for lk in list_lk:
            lk_df = pd.DataFrame(index=pd.date_range((str(year) + '-01-01'),
                                                     periods=hours, freq='H'))
            tw_df_lk = tw_df.loc[:, int(lk)]
            tw_df_lk.index = pd.DatetimeIndex(tw_df_lk.index)
            last_hour = tw_df_lk.copy()[-1:]
            last_hour.index = last_hour.index + timedelta(1)
            tw_df_lk = tw_df_lk.append(last_hour)
            tw_df_lk = tw_df_lk.resample('H').pad()
            tw_df_lk = tw_df_lk[:-1]

            temp_cal = temp_calender_df.copy()
            temp_cal = temp_cal[['Date', 'Tagestyp', lk]].set_index("Date")
            last_hour = temp_cal.copy()[-1:]
            last_hour.index = last_hour.index + timedelta(1)
            temp_cal = temp_cal.append(last_hour)
            temp_cal = temp_cal.resample('H').pad()
            temp_cal = temp_cal[:-1]
            temp_cal['Stunde'] = pd.DatetimeIndex(temp_cal.index).time
            temp_cal = temp_cal.set_index(["Tagestyp", lk, 'Stunde'])

            for slp in list(dict.fromkeys(slp_wz_g().values())):
                f = ('Lastprofil_{}.xls'.format(slp))
                slp_profil = pd.read_excel(data_in('temporal',
                                                   'Gas Load Profiles', f))
                slp_profil = pd.DataFrame(slp_profil.set_index(
                    ['Tagestyp', 'Temperatur\nin °C\nkleiner']))
                slp_profil.columns = pd.to_datetime(slp_profil.columns,
                                                    format='%H:%M:%S')
                slp_profil.columns = pd.DatetimeIndex(slp_profil.columns).time
                slp_profil = slp_profil.stack()
                temp_cal['Prozent'] = [slp_profil[x] for x in temp_cal.index]
                for wz in [k for k, v in slp_wz_g().items() if v.startswith(slp)]:
                    lk_df[str(lk) + '_' + str(wz)] = (tw_df_lk[wz].values
                                                      * temp_cal['Prozent']
                                                      .values/100)
                    df[str(lk) + '_' + str(wz)] = (tw_df_lk[wz].values
                                                   * temp_cal['Prozent']
                                                   .values/100)

            df[str(lk)] = lk_df.sum(axis=1)
        if detailed:
            df = df.drop(columns=gv_lk.index.astype(str))
            df.columns =\
                pd.MultiIndex.from_tuples([(int(x), int(y)) for x, y in
                                           df.columns.str.split('_')])
        else:
            df = df[gv_lk.index.astype(str)]
            df.columns = df.columns.astype(int)
        # save to hard drive
        df.to_csv(path)

        if use_nuts3code:
            df = df.rename(columns=dict_region_code(level='lk', keys='ags_lk',
                                                    values='natcode_nuts3'),
                           level=(0 if detailed else None))
        return df