import pandas as pd

from src.data_processing.consumption import *
from src.data_processing.employees import get_employees_per_industry_sector_and_regional_ids
from src.configs.config_loader import load_config
from src.data_access.local_reader import *

# Pipeline functions combining the data_processing functions to generate wanted the data

def get_consumption_data_historical(year: int) -> pd.DataFrame: #TODO: iterativer approach
    """
    Get historical consumption data (2000-2018) for a specific year: Consumption per industry_sector [88?] and regional_id [401]
    
    Args:
        year (int): The year to get consumption data for
    """

    # 1. validate the year
    if year < 2000 or year > 2018:
        raise ValueError("Year must be between 2000 and 2018")

    # 2. Get UGR data
    # gas does also include other gases
    # gas does not include self generation, power does
    # not single industry_sectors, there are also industry_sector ranges/ Produktionsbereiche
    ugr_data_ranges = get_ugr_data_ranges(year)

    # 2. resolve the industry ranges
    employees = get_employees_per_industry_sector_and_regional_ids(year)

    # 3. resolve the ugr data ranges by employees
    ugr_data = resolve_ugr_industry_sector_ranges_by_employees(ugr_data_ranges, employees)

    # 3. fix gas: original source (GENISIS) gives sum of natural gas and other gases use factor from sheet to get natural gas only
    # TODO check wz 35 
    decomposition_factors_gas = load_decomposition_factors_gas()
    factor_natural_gas = decomposition_factors_gas['share_natural_gas_total_gas']
    ugr_data['gas[MWh]'] = (ugr_data['gas[MWh]'] * factor_natural_gas)


    # 3. add self consumption/ self gen for power and gas (baseed on power self generation)
    # Include the power and gas self generation
    # based on the power generation from self generation
    # I have the total gas self gen value but not how it is distributed across the WZs
    # I calculate the factor: How much of the total power self generation is in each WZ 
    # and assume that I can use that factor also for gas

    # self gen is only missing for gas, we get the total gas self consumption from JEVI. For power selfgen is already included
    total_gas_self_consuption = get_total_gas_industry_self_consuption(year)
    consumption_data, factor_power_selfgen, factor_gas_no_selfgen = calculate_self_generation(ugr_data, total_gas_self_consuption, load_decomposition_factors_power(), year)






    # 4. fix the industry consumption with iterative approach and resolve consumption to regional_ids
    

    # get regional energy consumption from JEVI
    regional_energy_consumption_jevi = get_regional_energy_consumption(year)

    # calculate the regional energy consumption iteratively
    """
    !!! uning "calculate_regional_energy_consumption" would be the cleaner and more efficient approach, but for now we are using the old dissaggregator approch
    consumption_data_power = calculate_regional_energy_consumption(consumption_data, 'power', year, regional_energy_consumption_jevi, employees)
    consumption_data_gas = calculate_regional_energy_consumption(consumption_data.loc[:, 'gas[MWh]'], year, regional_energy_consumption_jevi, employees)
    consumption_data_petrol = calculate_regional_energy_consumption(consumption_data.loc[:, 'petrol[MWh]'], year, regional_energy_consumption_jevi, employees)
    """
    # the old dissaggregator approach:
    consumption_data_power, consumption_data_gas = calculate_iteratively_industry_regional_consumption(sector_energy_consumption_ugr=consumption_data, regional_energy_consumption_jevi=regional_energy_consumption_jevi, employees_by_industry_sector_and_regional_ids=employees)




    """
    3 dfs: consumption for power, gas, petrol for years 2000-2018
    400 columns = regional_id
    88 columns = industry_sectors
    
    """
    return consumption_data


def get_consumption_data_future(year: int) -> pd.DataFrame: #TODO

    """
    Get historical consumption data (2000-2018) for a specific year: Consumption per industry_sector [88?] and regional_id [401]
    
    Args:
        year (int): The year to get consumption data for
    """
    ugr_genisis_year_end = load_config("base_config.yaml")["ugr_genisis_year_end"]


    # 1. validate the year
    if year < ugr_genisis_year_end or year > 2050:
        raise ValueError("Year must be between ugr_genisis_year_end and 2050")

    # 2. Get UGR data
    ugr_data_ranges = get_ugr_data_ranges(ugr_genisis_year_end)

    # project consumption

    # 2. resolve the industry ranges
    employees = get_employees_per_industry_sector_and_regional_ids(year)

    ugr_data = resolve_ugr_industry_sector_ranges_by_employees(ugr_data_ranges, employees)
    # project consumption
    consumption = project_consumption(ugr_data, ugr_genisis_year_end, year)

    # 3. add self consumption/ self gen

    # 4. fix the industry consumption with iterative approach




    return ugr_data


def get_consumption_data(year: int) -> pd.DataFrame:
    """
    Get consumption data for a specific year.
    """
    if year < 2000 or year > 2050:
        raise ValueError("Year must be between 2000 and 2050")
    
    if year <= 2018:
        return get_consumption_data_historical(year)
    else:
        return get_consumption_data_future(year)


def get_consumption_data_per_cts_or_industry(year: int, cts_or_industry: str) -> pd.DataFrame:
    """
    Get consumption data for a specific year and filter it per cts or industry
    = spacial.disagg_CTS_industry()

    Args:
        year (int): The year to get consumption data for
        cts_or_industry (str): 'cts' or 'industry'
    """
    # 1. validate the year and cts_or_industry
    if year < 2000 or year > 2050:
        raise ValueError("Year must be between 2000 and 2050")
    if cts_or_industry not in ['cts', 'industry']:
        raise ValueError("cts_or_industry must be 'cts' or 'industry'")


    # 2. get the consumption data
    consumption_data = get_consumption_data(year)

    # 3. filter the consumption data
    filtered = filter_consumption_data_per_cts_or_industry(consumption_data, cts_or_industry)

    return filtered
