def temporal_industry_elec_load_from_fuel_switch(df_temp_gas_switch,
                                                 p_ground=0.36, p_air=0.58,
                                                 p_water=0.06):
    """
    Calculates electric consumption temporally disaggregated gas consumption,
    which will be switched to power.

    Parameters
    -------
    df_temp_gas_switch : pd.DataFrame()
        timestamp as index, multicolumns with nuts-3, branch and applications.
        contains temporally disaggregated gas demand for fuel switch
    p_ground, p_air, p_water : float, default 0.36, 0.58, 0.06
        percentage of ground/air/water heat pumps sum must be 1

    Returns
    -------
    pd.DataFrame : 3 DataFrames with electricity consumption
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

    # read share of electrode heating system for heat between 100°C and 200°C
    PATH = data_in("dimensionless", "fuel_switch_keys.xlsx")
    df_electrode = pd.read_excel(PATH,
                                 sheet_name=("Gas2Power industry electrode"))
    df_electrode = (df_electrode
                    .loc[[isinstance(x, int) for x in df_electrode["WZ"]]]
                    .set_index("WZ")
                    .copy())

    # 1: get the COP timeseries for indoor heating --> T=40°C
    air_floor_cop, ground_floor_cop, water_floor_cop = cop_ts(sink_t=40,
                                                              source='ambient',
                                                              year=year)
    # assert that the years of COP TS and year of df_temp are aligned
    assert (air_floor_cop.index.year.unique() ==
            df_temp_gas_switch.index.year.unique()),\
        ("The year of COP ts does not match the year of the heat demand ts")

    # select indoor heating demand to be converted to electric demand with cop.
    # use efficiency to convert from gas to heat.
    df_hp_heat = (df_temp_gas_switch
                  .loc[:, col[:, :, ['Raumwärme']]]
                  * get_efficiency_level('Raumwärme'))
    df_temp_hp_heating = (p_ground * (df_hp_heat.div(ground_floor_cop, level=0)
                                      .fillna(method='ffill'))
                          + p_air * (df_hp_heat
                                     .div(air_floor_cop, level=0)
                                     .fillna(method='ffill'))
                          + p_water * (df_hp_heat
                                       .div(water_floor_cop, level=0)
                                       .fillna(method='ffill')))

    # 2: get the COP timeseries for low temperature process heat --> T=80°C
    air_floor_cop, ground_floor_cop, water_floor_cop = cop_ts(sink_t=80,
                                                              source='ambient',
                                                              year=year)
    # select low temperature heat to be converted to electric demand with cop
    df_hp_heat = (df_temp_gas_switch
                  .loc[:, col[:, :, ['Prozesswärme <100°C']]]
                  * get_efficiency_level('Prozesswärme <100°C'))
    df_temp_hp_low_heat = (p_ground * (df_hp_heat
                                       .div(ground_floor_cop, level=0)
                                       .fillna(method='ffill'))
                           + p_air * (df_hp_heat
                                      .div(air_floor_cop, level=0)
                                      .fillna(method='ffill'))
                           + p_water * (df_hp_heat
                                        .div(water_floor_cop, level=0)
                                        .fillna(method='ffill')))

    # 3: get the COP timeseries for high temperature process heat
    # Use 2 heat pumps to reach this high temperature level
    # 3.1: 1st stage: T_sink = 60°C
    air_floor_cop, ground_floor_cop, water_floor_cop = cop_ts(sink_t=60,
                                                              source='ambient',
                                                              year=year)
    # select heat demand to be converted to electric demand with cop
    df_hp_heat = ((df_temp_gas_switch
                   .loc[:, col[:, :, ['Prozesswärme 100°C-200°C']]]
                   * get_efficiency_level('Prozesswärme 100°C-200°C'))
                  .multiply((1-df_electrode['Prozesswärme 100°C-200°C']),
                            axis=1, level=1))
    df_temp_hp_medium_heat_stage1 = (p_ground * (df_hp_heat
                                                 .div(ground_floor_cop,
                                                      level=0)
                                                 .fillna(method='ffill'))
                                     + p_air * (df_hp_heat
                                                .div(air_floor_cop, level=0)
                                                .fillna(method='ffill'))
                                     + p_water * (df_hp_heat
                                                  .div(water_floor_cop,
                                                       level=0)
                                                  .fillna(method='ffill')))
    # 3.2: 2nd stage: use heat from first stage
    # T_sink = 120°C, T_source = 60°C delta_T = 60°C
    high_temp_hp_cop = cop_ts(source='waste heat', delta_t=60, year=year)
    # select heat to be converted to electric demand with cop
    df_temp_hp_medium_heat_stage2 = (df_hp_heat.div(high_temp_hp_cop, level=0)
                                     .fillna(method='ffill'))
    # add energy consumption of both stages
    df_temp_hp_medium_heat = (df_temp_hp_medium_heat_stage1
                              .add(df_temp_hp_medium_heat_stage2,
                                   fill_value=0))

    # 4 calculate electric demand for electrode heaters
    df_electrode_switch_200 = ((df_temp_gas_switch
                            .loc[:, col[:, :, ['Prozesswärme 100°C-200°C']]]
                            * get_efficiency_level('Prozesswärme 100°C-200°C'))
                           .multiply((df_electrode['Prozesswärme 100°C-200°C']),
                                     axis=1, level=1)
                           / 0.98)  # HACK! 0.98 = electrode heater efficiency
    df_electrode_switch_500 = ((df_temp_gas_switch
                                .loc[:, col[:, :, ['Prozesswärme 200°C-500°C']]]
                                * get_efficiency_level('Prozesswärme 200°C-500°C'))
                               .multiply((df_electrode['Prozesswärme 200°C-500°C']),
                                         axis=1, level=1)
                               / 0.98)  # HACK! 0.98 = electrode heater efficiency
    df_electrode_switch = df_electrode_switch_200.join(df_electrode_switch_500)

    # 5 get the COP timeseries for warm water  --> T=55°C
    air_floor_cop, ground_floor_cop, water_floor_cop = cop_ts(sink_t=55,
                                                              source='ambient',
                                                              year=year)

    # select warm water heat to be converted to electric demand with cop
    df_hp_heat = (df_temp_gas_switch
                  .loc[:, col[:, :, ['Warmwasser']]]
                  * get_efficiency_level('Warmwasser'))
    df_temp_warm_water = (p_ground * (df_hp_heat.div(ground_floor_cop, level=0)
                                      .fillna(method='ffill'))
                          + p_air * (df_hp_heat.div(air_floor_cop, level=0)
                                     .fillna(method='ffill'))
                          + p_water * (df_hp_heat
                                       .div(water_floor_cop, level=0)
                                       .fillna(method='ffill')))
    # 6 select Mechanical Energy
    df_mechanical_switch = ((df_temp_gas_switch
                            .loc[:, col[:, :, ['Mechanische Energie']]])
                            * (get_efficiency_level('Mechanische Energie')
                               / 0.9))  # HACK! 0.9 = electric motor efficiency

    # # 7 select Industrial power plants
    # df_self_gen_switch = ((df_temp_gas_switch
    #                        .loc[:, col[:, :, ['Industriekraftwerke']]])
    #                       * 0.35)  # TBD HACK! Update get_efficiency_level()

    # add all dataframes together for electric demand per nuts3, branch and app
    df_temp_elec_from_gas_switch = (df_temp_elec_from_gas_switch
                                    .add(df_temp_hp_heating, fill_value=0)
                                    .add(df_temp_hp_low_heat, fill_value=0)
                                    .add(df_temp_hp_medium_heat, fill_value=0)
                                    .add(df_temp_warm_water, fill_value=0)
                                    .add(df_mechanical_switch, fill_value=0)
                                    .add(df_electrode_switch, fill_value=0))
    # df_temp_elec_from_gas_switch = (df_temp_elec_from_gas_switch
    #                                 .loc[:, (df_temp_elec_from_gas_switch != 0)
    #                                      .any(axis=0)])
    

    return df_temp_elec_from_gas_switch#, df_temp_hp_medium_heat, df_electrode_switch]