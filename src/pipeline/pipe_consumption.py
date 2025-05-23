import pandas as pd

from src.data_processing.consumption import *
from src.data_processing.employees import get_employees_per_industry_sector_and_regional_ids
from src.configs.config_loader import load_config
from src.data_access.local_reader import *
from src.data_processing.effects import *
# Pipeline functions combining the data_processing functions to generate wanted the data


# main function with cache and energy carrier
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
            logger.info(f"Loading from cache: get_consumption_data(year={year}, energy_carrier={energy_carrier})")
            return consumption_data
    



    # 2. get the consumption data: historical or projected in the future
    consumption_data_power, consumption_data_gas, consumption_data_petrol = get_consumption_data_historical_and_future(year)


    # 3. return the correct consumption data for the energy carrier
    if energy_carrier == "power":
        consumption_data = consumption_data_power
    elif energy_carrier == "gas":
        consumption_data = consumption_data_gas
    elif energy_carrier == "petrol":
        consumption_data = consumption_data_petrol
    else:
        raise ValueError("Energy carrier must be 'power' or 'gas' or 'petrol'")   

    
    # validation: check if there are no NaN values
    if consumption_data.isnull().any().any():
        raise ValueError("consumption_data contains NaN values")
    

    # 4. save to cache
    logger.info(f"Saving consumption data {energy_carrier} for year {year} to cache...")
    processed_dir = load_config("base_config.yaml")['consumption_data_cache_dir']
    processed_file = os.path.join(processed_dir, load_config("base_config.yaml")['consumption_data_cache_file'].format(energy_carrier=energy_carrier, year=year))
    os.makedirs(processed_dir, exist_ok=True)
    consumption_data.to_csv(processed_file)
    logger.info(f"Cached: get_consumption_data(year={year}, energy_carrier={energy_carrier} saved to {processed_file}")

    return consumption_data


# fiter get_consumption_data() for cts or industry
def get_consumption_data_per_indsutry_sector_energy_carrier(year: int, cts_or_industry: str, energy_carrier: str, force_preprocessing: bool = True) -> pd.DataFrame:
    """
    Get consumption data for a specific year and filter it per cts or industry
    = spacial.disagg_CTS_industry()

    Args:
        year (int): The year to get consumption data for
        cts_or_industry (str): 'cts' or 'industry'
        energy_carrier (str): 'power' or 'gas' or 'petrol'
        force_preprocessing (bool): If True, the data will be preprocessed even if a cache file exists
    """
    # 1. validate the year and cts_or_industry
    if year < 2000 or year > 2045:
        raise ValueError("Year must be between 2000 and 2045")
    if cts_or_industry not in ['cts', 'industry']:
        raise ValueError("cts_or_industry must be 'cts' or 'industry'")
    if energy_carrier not in ['power', 'gas', 'petrol']:
        raise ValueError("energy_carrier must be 'power' or 'gas' or 'petrol'")


    # 2. get the consumption data
    consumption_data = get_consumption_data(year=year, energy_carrier=energy_carrier, force_preprocessing=force_preprocessing)


    # 3. filter the consumption data
    filtered_consumption_data = filter_consumption_data_per_cts_or_industry(consumption_data, cts_or_industry)


    return filtered_consumption_data



def get_consumption_data_historical_and_future(year: int) -> pd.DataFrame:
    """
    Get historical and projected consumption data (2000-2050) for a specific year: Consumption per industry_sector [88?] and regional_ids [400]
    
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

        ->     3 dfs: consumption for power, gas, petrol for years 2000-2050
                400 columns = regional_id
                88 columns = industry_sectors
    """
    # 0.1. get the ugr_genisis_year_end = year of the last UGR data
    ugr_genisis_year_end = load_config("base_config.yaml")["ugr_genisis_year_end"]
    year_for_projection = None

    # 0. validate the year
    if year < 2000 or year > 2045:
        raise ValueError("Year must be between 2000 and 2045")
    

    # 1. set the years for getting the URG data and for projection
    if year > ugr_genisis_year_end:
        year_for_projection = year
        year = ugr_genisis_year_end

    
    # 1. Get the raw UGR data
    # gas does also include other gases
    # gas does not include self generation, power does
    # not single industry_sectors, there are also industry_sector ranges/ Produktionsbereiche
    ugr_data_ranges = get_ugr_data_ranges(year)


    if year_for_projection is not None:
        # apply activity drivers (Mengeneffekt) to project the consumption into the future
        ugr_data_ranges = apply_activity_driver(ugr_data_ranges, ugr_genisis_year_end, year_for_projection)


    # 2. resolve the industry_sector range ranges by employees
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
    decomposition_factors_power = load_decomposition_factors_power()
    consumption_data, factor_power_selfgen, factor_gas_no_selfgen = calculate_self_generation(ugr_data, total_gas_self_consuption, decomposition_factors_power, year_for_projection)


    # 6. fix the industry consumption with iterative approach and dissaggregate the consumption to regional_ids
    # 6.1 get regional energy consumption from JEVI
    regional_energy_consumption_jevi = get_regional_energy_consumption(year)

    # 6.2 calculate the regional energy consumption iteratively
    # the old dissaggregator approach: returns the total consumption for power, gas and petrol per regional_id and industry_sector
    consumption_data_power, consumption_data_gas, consumption_data_petrol = calculate_iteratively_industry_regional_consumption(sector_energy_consumption_ugr=consumption_data, regional_energy_consumption_jevi=regional_energy_consumption_jevi, employees_by_industry_sector_and_regional_ids=employees)
    
    # 6.3 set the index and columns names
    consumption_data_power.index.name = 'industry_sector'
    consumption_data_gas.index.name = 'industry_sector'
    consumption_data_petrol.index.name = 'industry_sector'
    consumption_data_power.columns.name = 'regional_id'
    consumption_data_gas.columns.name = 'regional_id'
    consumption_data_petrol.columns.name = 'regional_id'


    # validation: check if there are no NaN values
    if consumption_data_power.isnull().any().any():
        raise ValueError("consumption_data_power contains NaN values")
    if consumption_data_gas.isnull().any().any():
        raise ValueError("consumption_data_gas contains NaN values")
    if consumption_data_petrol.isnull().any().any():
        raise ValueError("consumption_data_petrol contains NaN values")



    return consumption_data_power, consumption_data_gas, consumption_data_petrol




