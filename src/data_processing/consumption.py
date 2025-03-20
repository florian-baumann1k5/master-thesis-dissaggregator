import pandas as pd
from src import logger

from src.configs.config_loader import load_config



import holidays
import datetime
from collections import OrderedDict
from collections.abc import Iterable


def generate_specific_consumption_per_branch(year=2018, **kwargs):
    """
    NEW
    Returns specific power and gas consumption per branch and year.
        2001-2019:  data from openFfE API
        2018:       data from local pre-calculated data
        2019-2050:  data from local pre-calculated data (2019). !Projecting future consumption using activity drivers from input files

    Also returns total power and gas consumption per branch.
    Also returns the amount of workers per branch and district.
    Also returns the factor of power and gas self generation per branch.

    Returns
    ------------
    dataframe with the following columns:±
    WZ ['power_incl_selfgen[MWh]', 'employees', 'factor_power_no_selfgen', 'gas_incl_selfgen[MWh]', 'factor_gas_no_selfgen']
    88 rows x 5 columns
    """
    logger.info('Function call:', extra={'function': 'generate_specific_consumption_per_branch_flo', 'kwargs': kwargs})

    
    # get a year - if not in kwargs, get it from config
    cfg = kwargs.get('cfg', load_config("base_config.yaml"))
    year = kwargs.get('year', cfg['base_year'])
    # get electricity and gas consumption from database
    x = True
    year1 = year
    # input validation
    if year1 not in range(2000, 2051):
        raise ValueError("`year` must be between 2000 and 2050")
    
    ## get consumption data
    consumption_by_wz = get_total_gas_power_consumption_by_wz(year=year)
    """ consumption_by_wz_ranges:
    WZ  gas[MWh]    power[MWh]
    88 rows x 2 columns
    """
    


    ## calculate specific consumption 
    # spez consumption = consumption / number of employees
    # see Diss S. 73

    employees_by_WZ_and_regional_code = pd.DataFrame(employees_per_branch(year=year))
    """
    88 rows x 401 columns
    """

    # sum up the columns of employees_by_WZ_and_regional_code to get total employees per WZ
    employees_by_WZ = employees_by_WZ_and_regional_code.sum(axis=1).rename("employees")
    """
    88 rows x 1 column
    """

    # merge the employees_by_WZ with the consumption_by_wz
    consumption_and_employees_by_WZ = consumption_by_wz.merge(employees_by_WZ, left_index=True, right_index=True)
    """
    88 rows x 3 column
    columns: gas[MWh] power[MWh] employees
    index: WZ
    """
    
    

    ## add self generation to the consumption_and_employees_by_WZ
    # with the help of the decomposition factors

    # get the decomposition factors
    file_name = "Decomposition Factors.xlsx"
    df_decom_el = pd.read_excel(data_in("dimensionless", file_name), sheet_name="Endenergieverbrauch Strom" )
    df_decom_el.set_index("WZ", inplace=True)
    df_decom_gas = pd.read_excel(data_in("dimensionless", file_name), sheet_name="Endenergieverbrauch Gas" )
    df_decom_gas.set_index("WZ", inplace=True)
    # create new DataFrame with columns from df_decom_el and df_decom_gas
    decomposition_factors_by_WZ = pd.concat(
        [
            df_decom_el[["Strom Netzbezug", "Strom Eigenerzeugung"]],
            df_decom_gas["Anteil Erdgas am Verbrauch aller Gase"],
        ],
        axis=1,
    )
    # fill missing values with 0 and 1 (WZ 35)
    decomposition_factors_by_WZ["Strom Eigenerzeugung"].fillna(0, inplace=True)
    decomposition_factors_by_WZ.fillna(1, inplace=True)


    ## add filter to only look at "Erdgas", ignore the other gases
    consumption_and_employees_by_WZ['gas[MWh]'] = (consumption_and_employees_by_WZ['gas[MWh]'] * decomposition_factors_by_WZ['Anteil Erdgas am Verbrauch aller Gase'])


    # merge the decomposition factors with the consumption_and_employees_by_WZ
    consumption_and_employees_and_decomfactors_by_WZ = consumption_and_employees_by_WZ.merge(decomposition_factors_by_WZ, left_index=True, right_index=True)
    """ consumption_and_employees_and_decomfactors_by_WZ
    WZ ['gas[MWh]', 'power[MWh]', 'employees', 'Strom Netzbezug', 'Strom Eigenerzeugung', 'Anteil Erdgas am Verbrauch aller Gase']
    88 rows x 6 columns
    """





    ## Get the gas consumption for self generation from local files
    # original source (table_id = 71) does not include gas consumption for
    # self generation in industrial sector
    # get gas consumption for self_generation from German energy balance JEVI
    gas_industry_self_consuption = get_gas_industry_self_consuption(year)



    ## Include the power and gas self generation
    # based on the power generation from self generation
    # I have the total gas self gen value but not how it is distributed across the WZs
    # I calculate the factor: How much of the total power self generation is in each WZ 
    # and assume that I can use that factor also for gas

    # self gen is only missing for gas, we get that consumption from JEVI. For power it is already included



    temp_df = consumption_and_employees_and_decomfactors_by_WZ.copy()

    # rename column sincel power already includes selfgen and gas does not
    temp_df.rename(columns={"power[MWh]": "power_incl_selfgen[MWh]", "gas[MWh]": "gas_no_selfgen[MWh]"}, inplace=True)

    temp_df["power_only_selfgen[MWh]"] = temp_df["Strom Eigenerzeugung"] * temp_df["power_incl_selfgen[MWh]"]
    temp_df["factor_selfgen_of_total_power"] = temp_df["power_only_selfgen[MWh]"] / temp_df["power_only_selfgen[MWh]"].sum()

    # use power factor on gas total consumption to get gas self generation per wz
    temp_df["gas_only_selfgen[MWh]"] = temp_df["factor_selfgen_of_total_power"] * gas_industry_self_consuption
    temp_df["gas_incl_selfgen[MWh]"] = temp_df["gas_no_selfgen[MWh]"] + temp_df["gas_only_selfgen[MWh]"]
   

    temp_df["factor_gas_no_selfgen"] = temp_df["gas_no_selfgen[MWh]"] / temp_df["gas_incl_selfgen[MWh]"]
    temp_df.rename(columns={"Strom Netzbezug": "factor_power_no_selfgen"}, inplace=True)


    ## dataframe cleanup
    consumption_employees_no_selfgen_by_WZ = temp_df.copy()
    consumption_employees_no_selfgen_by_WZ.drop(columns=['Strom Eigenerzeugung', 
                                                                   'Anteil Erdgas am Verbrauch aller Gase', 
                                                                   'power_only_selfgen[MWh]',
                                                                   'factor_selfgen_of_total_power', 
                                                                   'gas_only_selfgen[MWh]',
                                                                   'gas_no_selfgen[MWh]'], inplace=True)
    """
    WZ ['power_incl_selfgen[MWh]', 'employees', 'factor_power_no_selfgen', 'gas_incl_selfgen[MWh]', 'factor_gas_no_selfgen']
    88 rows x 5 columns
    """

    # Set the index name to "WZ" before returning
    consumption_employees_no_selfgen_by_WZ.index.name = "WZ"
    return consumption_employees_no_selfgen_by_WZ