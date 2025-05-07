def temporal_cts_elec_load_from_fuel_switch(df_temp_gas_switch, p_ground=0.36,
                                            p_air=0.58, p_water=0.06):
    """
    Converts timeseries of gas demand per NUTS-3 and branch and application to
        electric consumption timeseries. Uses COP timeseries for heat
        applications. uses efficiency for mechanical energy.
    Parameters
    -------
    df_temp_gas_switch : pd.DataFrame()
        timestamp as index, multicolumns with nuts-3, branch and applications.
        contains temporally disaggregated gas demand for fuel switch
    p_ground, p_air, p_water : float, default 0.36, 0.58, 0.06
        percentage of ground/air/water heat pumps sum must be 1
    Returns
    -------
    None.

    """
    if p_ground + p_air + p_water != 1:
        raise ValueError("sum of percentage of ground/air/water heat pumps"
                         " must be 1")
    # get year from dataframe index
    year = df_temp_gas_switch.index[0].year
    # create new DataFrame for results
    df_temp_elec_from_gas_switch = pd.DataFrame(index=df_temp_gas_switch.index,
                                                columns=(df_temp_gas_switch
                                                         .columns),
                                                data=0)
    # create index slicer for data selection
    col = pd.IndexSlice
    # select heat applications which will be converted using cop series for
    # different temperatur levels. use efficiency to convert from gas to heat.
    # select indoor heating
    df_heating_switch = (df_temp_gas_switch.loc[:, col[:, :, ['Raumwärme']]]
                         * get_efficiency_level('Raumwärme'))
    # get the COP timeseries for indoor heating --> T=40°C
    air_floor_cop, ground_floor_cop, water_floor_cop = cop_ts(sink_t=40,
                                                              source='ambient',
                                                              year=year)
    # assert that the years of COP TS and year of df_temp are aligned
    assert (air_floor_cop.index.year.unique() ==
            df_heating_switch.index.year.unique()),\
        ("The year of COP ts does not match the year of the heat demand ts")
    df_temp_indoor_heating = (p_ground * (df_heating_switch
                                          .div(ground_floor_cop, level=0)
                                          .fillna(method='ffill'))
                              + p_air * (df_heating_switch
                                         .div(air_floor_cop, level=0)
                                         .fillna(method='ffill'))
                              + p_water * (df_heating_switch
                                           .div(water_floor_cop, level=0)
                                           .fillna(method='ffill')))
    # select process heating
    df_heating_switch = (df_temp_gas_switch.loc[:, col[:, :, ['Prozesswärme']]]
                         * get_efficiency_level('Prozesswärme'))
    # get the COP timeseries for process heating --> T=70°C
    air_floor_cop, ground_floor_cop, water_floor_cop = cop_ts(sink_t=70,
                                                              source='ambient',
                                                              year=year)
    df_temp_process_heat = (p_ground * (df_heating_switch.div(ground_floor_cop,
                                                              level=0)
                                        .fillna(method='ffill'))
                            + p_air * (df_heating_switch.div(air_floor_cop,
                                                             level=0)
                                       .fillna(method='ffill'))
                            + p_water * (df_heating_switch
                                         .div(water_floor_cop, level=0)
                                         .fillna(method='ffill')))
    # select warm water heating
    df_heating_switch = (df_temp_gas_switch.loc[:, col[:, :, ['Warmwasser']]]
                         * get_efficiency_level('Warmwasser'))
    # get the COP timeseries for warm water  --> T=55°C
    air_floor_cop, ground_floor_cop, water_floor_cop = cop_ts(sink_t=55,
                                                              source='ambient',
                                                              year=year)
    df_temp_warm_water = (p_ground * (df_heating_switch.div(ground_floor_cop,
                                                            level=0)
                                      .fillna(method='ffill'))
                          + p_air * (df_heating_switch.div(air_floor_cop,
                                                           level=0)
                                     .fillna(method='ffill'))
                          + p_water * (df_heating_switch
                                       .div(water_floor_cop, level=0)
                                       .fillna(method='ffill')))
    # select Mechanical Energy
    df_mechanical_switch = ((df_temp_gas_switch
                            .loc[:, col[:, :, ['Mechanische Energie']]])
                            * (get_efficiency_level('Mechanische Energie')
                               / 0.9))  # HACK! 0.9 = electric motor efficiency
    # add all dataframes together for electric demand per nuts3, branch and app
    df_temp_elec_from_gas_switch = (df_temp_elec_from_gas_switch
                                    .add(df_temp_indoor_heating, fill_value=0)
                                    .add(df_temp_process_heat, fill_value=0)
                                    .add(df_temp_warm_water, fill_value=0)
                                    .add(df_mechanical_switch, fill_value=0))
    return df_temp_elec_from_gas_switch