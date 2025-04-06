import numpy as np

from src.data_access.local_reader import *
from src.configs.mappings import *



def dissaggregate_for_applications(consumption_data, year, sector, energy_carrier):
    """
    Dissaggregate the consumption data based on applications.

    Args:
        consumption_data (pd.DataFrame): Consumption data
        year (int): Year
        sector (str): Sector
        energy_carrier (str): Energy carrier

    Returns:
        pd.DataFrame: Disaggregated consumption data
            MultiIndex columns: (industry_sector, application)
            MultiIndex index: (regional_id)
    """

    # 1. load decomposition factors only for the relevant industry_sectors and energy_carrier
    decomp = get_application_dissaggregation_factors(sector=sector, energy_carrier=energy_carrier)

    # 2. call the correct function based on the energy_carrier; 
    # only gas-industry is an exception since we have to consider industrial_power_plants
    if energy_carrier == "gas" and sector == "industry":
        df =  disagg_applications_gas_industry(consumption_data, decomp, year)
        """
        400 rows x 261 columns 
        """
    
    elif (energy_carrier == "gas" and sector == "cts") or (energy_carrier == "power"):
        df =  disagg_applications_default(consumption_data, decomp)
        """ gas cts
        400 rows x 290 columns
        """
        """ power industry
        400 rows x 319 columns
        """
        """ power cts
        400 rows x 464 columns
        """

    elif energy_carrier == "petrol":
        df = disagg_applications_petrol(consumption_data, decomp)
        """
        """

    # 3. calidate the output
    new_df_sum = df.sum().sum()
    total_sum = consumption_data.sum().sum()
    msg = (
        f"The sum of the disaggregated consumption must be the same as the sum of the initial consumption data! "
        f"New sum: {new_df_sum}, initial consumption sum: {total_sum}"
    )
    assert np.isclose(total_sum, new_df_sum), msg

    return df


def disagg_applications_gas_industry(consumption_data, decomp_gas_temp, year):
    """
    Performes the disaggregation of the gas-industry consumption data based on applications.
    This is the only case where we have to consider industrial_power_plants.

    Args:
        consumption_data (pd.DataFrame): Consumption data: columns: industry_sectors, index: regional_ids ; already filtered to contain only relevant industry_sectors(cts/industry)
        decomp_gas_temp (pd.DataFrame): Decomposition factors for gas and temperature APPLICATIONS
        year (int): Year

    Returns:
        pd.DataFrame: Disaggregated consumption data with MultiIndex columns (industry_sector, application).
    """

    # 1. load factor_gas_no_selfgen factors
    factor_gas_no_selfgen = load_factor_gas_no_selfgen_cache(year)
    factor_gas_selfgen = 1 - factor_gas_no_selfgen


    # 2. calculate the gas consumption without and without self gen
    # With self gen
    if isinstance(factor_gas_selfgen, pd.DataFrame) and factor_gas_selfgen.shape[1] == 1:
        factor_gas_selfgen = factor_gas_selfgen.iloc[:, 0]
    factor_gas_selfgen = factor_gas_selfgen.reindex(consumption_data.index)
    consumption_data_selfgen = consumption_data.mul(factor_gas_selfgen, axis=0) # only self gen gas consumption 
    # Without self gen
    if isinstance(factor_gas_no_selfgen, pd.DataFrame) and factor_gas_no_selfgen.shape[1] == 1:
        factor_gas_no_selfgen = factor_gas_no_selfgen.iloc[:, 0]
    factor_gas_no_selfgen = factor_gas_no_selfgen.reindex(consumption_data.index)
    consumption_data_no_selfgen = consumption_data.mul(factor_gas_no_selfgen, axis=0) # gas consumption without self gen


    # 3. Transpose to get (regions x industry_sectors)
    consumption_data_no_selfgen = consumption_data_no_selfgen.T  # shape: (400 regions, 29 industry_sectors)


    # 4. Calculate disaggregated consumption for each application
    industry_sectors = consumption_data_no_selfgen.columns
    applications = decomp_gas_temp.columns
    regions = consumption_data_no_selfgen.index

    # 5. MultiIndex: (industry_sector, application)
    # https://pandas.pydata.org/docs/reference/api/pandas.MultiIndex.from_product.html
    column_index = pd.MultiIndex.from_product([industry_sectors, applications], names=["industry_sector", "application"])
    """ column_index
    MultiIndex:
    88 industry sectors * 8 applications = 704 columns
    """

    # 6. Initialize result dataframe
    new_df = pd.DataFrame(index=regions, columns=column_index)
    """ new_df
    empty dataframe:
    400 rows x 704 columns: 400 regions x 704 (industry sectors x applications)
    """

    # 5. Efficient fill: loop only over industry sectors    for industry in industry_sectors:
    for industry in industry_sectors:
        sector_consumption = consumption_data_no_selfgen[industry]  # Series: index = regions
        app_shares = decomp_gas_temp.loc[industry]               # Series: index = applications

        for application in applications:
            new_df[(industry, application)] = sector_consumption * app_shares[application]

    # 6. join now with industrial_power_plant consumption (consumption_data_selfgen)
    df_ipplant = consumption_data_selfgen.T  # shape: (regions, industry_sectors)
    # Convert columns to MultiIndex: (industry_sector, "industrial_power_plant")
    df_ipplant.columns = pd.MultiIndex.from_product(
        [df_ipplant.columns, ["industrial_power_plant"]],
        names=["industry_sector", "application"]
    )
    # 3. Join to new_df along columns
    new_df = new_df.join(df_ipplant)

    return new_df

    
def disagg_applications_default(consumption_data, decomp):
    """
    Performs the disaggregation of the consumption data based on applications.
    This is the default case.

    Args:
        consumption_data (pd.DataFrame): Consumption data
        decomp (pd.DataFrame): Decomposition factors for applications

    Returns:
        pd.DataFrame: Disaggregated consumption data with MultiIndex columns (industry_sector, application).
        MultiIndex columns: (industry_sector, application)
        MultiIndex index: (regional_id)
    """

    consumption_data = consumption_data.T  # shape: (400 regions, 29 industry_sectors)

    # 4. Calculate disaggregated consumption for each application
    industry_sectors = consumption_data.columns
    applications = decomp.columns
    regions = consumption_data.index

    # 5. MultiIndex: (industry_sector, application)
    # https://pandas.pydata.org/docs/reference/api/pandas.MultiIndex.from_product.html
    column_index = pd.MultiIndex.from_product([industry_sectors, applications], names=["industry_sector", "application"])
    """ column_index
    MultiIndex:
    88 industry sectors * 8 applications = 704 columns
    """

    # 6. Initialize result dataframe
    new_df = pd.DataFrame(index=regions, columns=column_index)
    """ new_df
    empty dataframe:
    400 rows x 704 columns: 400 regions x 704 (industry sectors x applications)
    """

    # 5. Efficient fill: loop only over industry sectors    for industry in industry_sectors:
    for industry in industry_sectors:
        sector_consumption = consumption_data[industry]  # Series: index = regions
        app_shares = decomp.loc[industry]               # Series: index = applications

        for application in applications:
            new_df[(industry, application)] = sector_consumption * app_shares[application]

    return new_df




def disagg_applications_petrol(): #TODO: add petrol
    """
    Perfrom dissagregation based on applications of the final energy usage
    """
    return None





def get_application_dissaggregation_factors(sector: str, energy_carrier: str): # TODO: add petrol   #TODO:  add cache?
    """
    Get the application dissaggregation factors for a given industry and energy carrier.
    (is the "usage" dataframe in the spatial.disagg_applications fct):

    energy_carrier == "gas" and industry == "industry"      8: ['non_energetic_use', 'mechanical_energy', 'space_heating', 'hot_water', 'process_heat_below_100C', 
                                                            'process_heat_100_to_200C', 'process_heat_200_to_500C', 'process_heat_above_500C']
    energy_carrier == "gas" and industry == "cts":          5: ['non_energetic_use', 'mechanical_energy', 'process_heat', 'space_heating', 'hot_water']
    energy_carrier == "power" and industry == "industry"    11: ['lighting', 'information_communication_technology', 'space_cooling', 'process_cooling', 
                                                            'mechanical_energy', 'space_heating', 'hot_water', 'process_heat_below_100C', 'process_heat_100_to_200C', 
                                                            'process_heat_200_to_500C', 'process_heat_above_500C']
    energy_carrier == "power" and industry == "cts"         8: ['lighting', 'information_communication_technology', 'space_cooling', 'process_cooling', 'mechanical_energy', 
                                                            'process_heat', 'space_heating', 'hot_water']


    Args:
        industry (str): Industry [industry, cts]
        energy_carrier (str): Energy carrier [gas, power]

    Returns:
        pd.DataFrame: Application dissaggregation factors
    """

    # 1. load decomposition factors and validate inputs
    if energy_carrier == "gas" and sector == "industry":
        decomp_temp = load_decomposition_factors_temperature_industry()
        decomp_gas = load_decomposition_factors_gas()

        # join the two dataframes
        decomp = decomp_gas.join(decomp_temp, how='inner')

        # since process_heat is the total sum of all process heat applications we have to multiply it
        # Multiply each of the share columns by the 'process_heat' column
        share_columns = [
            'process_heat_below_100C',
            'process_heat_100_to_200C',
            'process_heat_200_to_500C',
            'process_heat_above_500C'
        ]
        decomp[share_columns] = decomp[share_columns].multiply(decomp['process_heat'], axis=0)

        # drop unneeded columns
        if "process_heat" in decomp.columns:
            decomp = decomp.drop(columns={"process_heat", "share_natural_gas_total_gas", "natural_gas_consumption_energetic"})

    elif energy_carrier == "gas" and sector == "cts":
        decomp = load_decomposition_factors_gas()

        # only keep the cts sectors
        cts_sectors = dict_cts_or_industry_per_industry_sector()['cts']
        decomp = decomp.loc[decomp.index.intersection(cts_sectors)]

        # drop unneeded columns
        decomp = decomp.drop(columns={"share_natural_gas_total_gas", "natural_gas_consumption_energetic"})  

    elif energy_carrier == "power" and sector == "industry":
        decomp_power = load_decomposition_factors_power()
        decomp_temp = load_decomposition_factors_temperature_industry()

        # join dfs and only keep the industry sectors 5..33 
        decomp = decomp_power.join(decomp_temp, how='inner')
        # since process_heat is the total sum of all process heat applications we have to multiply it
        # Multiply each of the share columns by the 'process_heat' column
        share_columns = [
            'process_heat_below_100C',
            'process_heat_100_to_200C',
            'process_heat_200_to_500C',
            'process_heat_above_500C'
        ]
        decomp[share_columns] = decomp[share_columns].multiply(decomp['process_heat'], axis=0)

        # drop unneeded columns
        if "process_heat" in decomp.columns:
            decomp = decomp.drop(columns={"process_heat", "electricity_grid", "electricity_self_generation"})

    elif energy_carrier == "power" and sector == "cts":

        decomp = load_decomposition_factors_power()

        # only keep the cts sectors
        cts_sectors = dict_cts_or_industry_per_industry_sector()['cts']
        decomp = decomp.loc[decomp.index.intersection(cts_sectors)]

        # drop unneeded columns
        decomp = decomp.drop(columns={"electricity_grid", "electricity_self_generation"})
    
    else:
        raise ValueError(f"Energy carrier {energy_carrier} and/or industry {sector} not supported")
    
    return decomp


