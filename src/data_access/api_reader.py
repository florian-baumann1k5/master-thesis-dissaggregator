import pandas as pd


def get_total_gas_power_consumption_by_wz(year):
    """
    NEW
    Returns total power and gas consumption per WZ and year in MWh.

    2001-2019:  data from openFfE API
    2018-2019:  data from local pre-calculated data
    2020-2050:  Projecting future consumption using activity drivers from input files based on the year 2019

    Returns
    ------------
    pd.DataFrame with the consumption data by WZ
    88 rows x 4 columns
    WZ gas[MWh] power[MWh] gas[TJ] power[TJ]
    """

    # figure out the year of the data to be used:
    # openFfE API only has UGR data for the years 2001-2017
    # local pre-calculated data is available for the years 2018 and 2019
    # between 2018 and 2050, the data is taken from 2019
    if year in range(2001, 2018):

        # get "Umweltökonomische Gesamtrechnung, Tabelle 3.2.3.5, 2000..2017" from openFfE API
        # unit is TJ
        ugr_data_raw = database_get_flo('spatial', table_id=71, year=year)

        #rename the column to consumption [TJ]
        ugr_data_raw.rename(columns={'value': 'consumption[TJ]'}, inplace=True)

        
        # convert internal_id array to industry_id (Wirtschaftszweig = WZ) and energycarrier_id (Energieträger = ET) (bc of the API response table structure)
        # create two new columns "WZ" and "ET"
        # Extract WZ and ET from internal_id and add as new columns
        ugr_data_raw = ugr_data_raw.assign(
            WZ_api = ugr_data_raw['internal_id'].str[0],
            energy_carrier = ugr_data_raw['internal_id'].str[1]
        )

        # Filter for gas (12) and power (18) consumption and keep relevant columns
        ugr_data_raw_gas_power = (
            ugr_data_raw.loc[ugr_data_raw['energy_carrier'].isin([12, 18])]
            [['consumption[TJ]', 'WZ_api', 'energy_carrier']]
        )

        # Map WZ codes form openFfE API to standard classification
        # we have defined 49 WZs in the config file. Some of them are ranges
        wz_codes = wz_dict()
        consumption_gas_power_wz = (
            ugr_data_raw_gas_power
            .loc[ugr_data_raw_gas_power['WZ_api'].isin(wz_codes.keys())]
            .replace({'WZ_api': wz_codes})
            .rename(columns={'WZ_api': 'WZ'})
        )
        # sum up the consumption for wz 49 and the same energy carrier
        consumption_gas_power_wz = consumption_gas_power_wz.groupby(['WZ', 'energy_carrier'])['consumption[TJ]'].sum().reset_index()
        
        # add new column for consumption in MWh
        consumption_gas_power_wz = (
            consumption_gas_power_wz
            .assign(consumption_mwh = lambda x: x['consumption[TJ]'] * 1000 / 3.6)
            .rename(columns={'consumption_mwh': 'consumption[MWh]'})
        )
        # TODO: this is the old call with convertion and hardcoded values
        # todo: decide/ check if this is still necccessary
        # sv_wz_real, gv_wz_real = reshape_energy_consumption_df(consumption_gas_power_per_wz, year=year)


        # create new dataframe with energy carriers and their values
        # Pivot the consumption data to get gas and power consumption as columns
        consumption_by_wz = (consumption_gas_power_wz
            .pivot(index='WZ', 
                    columns='energy_carrier', 
                    values='consumption[MWh]')
            .rename(columns={12: 'gas[MWh]',
                            18: 'power[MWh]'})
        )
        """
        49 rows x 3 columns:
        WZ  gas[MWh]    power[MWh]
        """

    elif year == 2018 or year == 2019:
        consumption_by_wz = get_ugr_data_local(year)

        return consumption_by_wz

    elif year in range(2019, 2051):
        # TODO: refactor this -> Diss S. 63
        """
        new code:

        # latest year available
        year_dataset = 2019

        # get the consumptiondata for the latest year available
        consumption_by_wz = get_ugr_data_local(year_dataset)
        
        # split the dataframe into gas and power
        sv_wz_real = consumption_by_wz[['power[MWh]']]
        gv_wz_real = consumption_by_wz[['gas[MWh]']]
        """
        sv_wz_real, gv_wz_real = read_ugr_from_data_in(2019)
        # project energy demand per wz to given year using activity drivers from
        # input files. For industry gross value added (gva) per branch is used, for
        # CTS energy reference area per branch is used, which is derived from
        # projected number of employees
        sv_wz_real, gv_wz_real = (project_energy_consumption(
            sv_wz_real, gv_wz_real, year_projected=year, year_dataset=2019))
        
        # combine the two series into one dataframe
        consumption_by_wz = pd.concat([sv_wz_real, gv_wz_real], axis=1)

        # rename the columns
        consumption_by_wz.rename(columns={'SV WZ [MWh]': 'power[MWh]', 'GV WZ [MWh]': 'gas[MWh]'}, inplace=True)

        # add new row WZ 35 with 1 values
        # TODO: no idea why this is needed
        consumption_by_wz.loc[35] = [1, 1]
    
    else:
        raise ValueError("`year` must be between 2001 and 2050. Year: " + str(year))
    
    # Having the consumption data in the following format:
    """  consumption_by_wz
    49 rows x 3 columns:
    WZ  gas[MWh]    power[MWh]
       
    WZs still having ranges
    """ 


    ## Resolve WZ ranges based on employee distribution
    if any(isinstance(idx, str) and '-' in idx for idx in consumption_by_wz.index):
        # Get employee data for the specified year
        employees_by_WZ_and_regional_code = pd.DataFrame(employees_per_branch(year=year))
        # Resolve the WZ ranges using employee distribution
        consumption_by_wz = resolve_wz_ranges(
            consumption_by_wz, 
            employees_by_WZ_and_regional_code,
            year
        )

    # Result Validation:
    # we have defined 49 unique WZs in the config.wz_dict() , if less throw error
    if len(consumption_by_wz) != 88:
        raise ValueError(f"`consumption_by_wz` must have 49 WZs, but has {len(consumption_by_wz)}")
    if consumption_by_wz.isnull().values.any():
        raise ValueError("`consumption_by_wz` has missing values")

    return consumption_by_wz


