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

    Returns:
        [pd.DataFrame, pd.DataFrame]:
            - [0]: pd.DataFrame: power consumption
                - index: industry_sectors 88
                - columns: regional_ids 400
            - [1]: pd.DataFrame: gas consumption
                - index: industry_sectors 88
                - columns: regional_ids 400

        ->     3 dfs: consumption for power, gas, petrol for years 2000-2018
                400 columns = regional_id
                88 columns = industry_sectors
    """

    # 0. validate the year
    if year < 2000 or year > 2018:
        raise ValueError("Year must be between 2000 and 2018")


    # 1. Get the raw UGR data
    # gas does also include other gases
    # gas does not include self generation, power does
    # not single industry_sectors, there are also industry_sector ranges/ Produktionsbereiche
    ugr_data_ranges = get_ugr_data_ranges(year)


    # 2. resolve the industry ranges
    employees = get_employees_per_industry_sector_and_regional_ids(year)


    # 3. resolve the ugr data ranges by employees
    ugr_data = resolve_ugr_industry_sector_ranges_by_employees(ugr_data_ranges, employees)


    # 4. fix gas: original source (GENISIS) gives sum of natural gas and other gases use factor from sheet to get natural gas only
    decomposition_factors_gas = load_decomposition_factors_gas()
    factor_natural_gas = decomposition_factors_gas['share_natural_gas_total_gas']
    ugr_data['gas[MWh]'] = (ugr_data['gas[MWh]'] * factor_natural_gas)


    # 5. add self consumption/ self gen for power and gas (baseed on power self generation)
    # Include the power and gas self generation
    # based on the power generation from self generation
    # I have the total gas self gen value but not how it is distributed across the WZs
    # I calculate the factor: How much of the total power self generation is in each WZ 
    # and assume that I can use that factor also for gas
    # self gen is only missing for gas, we get the total gas self consumption from JEVI. For power selfgen is already included
    total_gas_self_consuption = get_total_gas_industry_self_consuption(year)
    consumption_data, factor_power_selfgen, factor_gas_no_selfgen = calculate_self_generation(ugr_data, total_gas_self_consuption, load_decomposition_factors_power(), year)


    # 6. fix the industry consumption with iterative approach and resolve consumption to regional_ids
    # get regional energy consumption from JEVI
    regional_energy_consumption_jevi = get_regional_energy_consumption(year)


    # 7. calculate the regional energy consumption iteratively
    """
    !!! uning "calculate_regional_energy_consumption" would be the cleaner and more efficient approach, but for now we are using the old dissaggregator approch
    consumption_data_power = calculate_regional_energy_consumption(consumption_data, 'power', year, regional_energy_consumption_jevi, employees)
    consumption_data_gas = calculate_regional_energy_consumption(consumption_data.loc[:, 'gas[MWh]'], year, regional_energy_consumption_jevi, employees)
    consumption_data_petrol = calculate_regional_energy_consumption(consumption_data.loc[:, 'petrol[MWh]'], year, regional_energy_consumption_jevi, employees)
    """
    # the old dissaggregator approach: returns the total consumption for power, gas and petrol per regional_id and industry_sector
    consumption_data_power, consumption_data_gas = calculate_iteratively_industry_regional_consumption(sector_energy_consumption_ugr=consumption_data, regional_energy_consumption_jevi=regional_energy_consumption_jevi, employees_by_industry_sector_and_regional_ids=employees)
    consumption_data_power.index.name = 'industry_sector'
    consumption_data_gas.index.name = 'industry_sector'
    consumption_data_power.columns.name = 'regional_id'
    consumption_data_gas.columns.name = 'regional_id'


    return consumption_data_power, consumption_data_gas


def get_consumption_data_future(year: int) -> pd.DataFrame: #TODO

    """
    Get historical consumption data (2000-2018) for a specific year: Consumption per industry_sector [88] and regional_id [400]
    
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



# main function (with cache)
def get_consumption_data(year: int, energy_carrier: str, force_preprocessing: bool = False) -> pd.DataFrame:
    """
    Get consumption data for a specific year.
    """


    # 0. validate the input
    if year < 2000 or year > 2050:
        raise ValueError("Year must be between 2000 and 2050")
    if energy_carrier not in ['power', 'gas', 'petrol']:
        raise ValueError("Energy carrier must be 'power' or 'gas' or 'petrol'")
    

    # 1. load from cache if not force_preprocessing and cache exists
    if not force_preprocessing:
         consumption_data = load_consumption_data_cache(year=year, energy_carrier=energy_carrier)

         if consumption_data is not None:
            logger.info(f"Loading from cache: result of get_consumption_data for energy_carrier: {energy_carrier}, year: {year}")
            return consumption_data
    

    # 2. get the consumption data either historical or future
    if year <= 2018:
        consumption_data_power, consumption_data_gas = get_consumption_data_historical(year)
    else:
        consumption_data_power, consumption_data_gas = get_consumption_data_future(year)


    # 3. return the correct consumption data for the energy carrier
    if energy_carrier == "power":
        consumption_data = consumption_data_power
    elif energy_carrier == "gas":
        consumption_data = consumption_data_gas
    #TODO Petrol: 
    # elif energy_carrier == "petrol":
    #     return consumption_data_petrol
    else:
        raise ValueError("Energy carrier must be 'power' or 'gas' or 'petrol'")    
    

    # 4. save to cache
    processed_dir = load_config("base_config.yaml")['consumption_data_cache_dir']
    processed_file = os.path.join(processed_dir, load_config("base_config.yaml")['consumption_data_cache_file'].format(energy_carrier=energy_carrier, year=year))
    os.makedirs(processed_dir, exist_ok=True)
    consumption_data.to_csv(processed_file)
    logger.info(f"Saving to cache: result of get_consumption_data for energy_carrier: {energy_carrier}, year: {year}")

    return consumption_data


# fiter get_consumption_data() for cts or industry
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
    consumption_data_power, consumption_data_gas = get_consumption_data(year)


    # 3. filter the consumption data
    filtered_power = filter_consumption_data_per_cts_or_industry(consumption_data_power, cts_or_industry)
    filtered_gas = filter_consumption_data_per_cts_or_industry(consumption_data_gas, cts_or_industry)

    return filtered_power, filtered_gas
