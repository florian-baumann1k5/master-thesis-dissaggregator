import pandas as pd

from src.data_processing.effects import *
from src.pipeline.pipe_consumption import *
from src.data_processing.application import *
from src.data_access.local_reader import *
"""
Dissaggregating the consumption data (per industry sector and regional_id) based on their applications.

disagg_applications_efficiency_factor()

"""

# main function (with cache)
def disagg_applications_efficiency_factor(sector: str, energy_carrier: str, year: int, force_preprocessing: bool = False) -> pd.DataFrame: #TODO
    """
    equals spacial.disagg_applications_eff() in old code

    Takes the current consumption data and dissaggragates it for applications and applies efficiency enhancement factors
    

    Args:
        sector (str): 'cts' or 'industry'
        energy_carrier (str): 'power' or 'gas'
        year (int): Year from 2000 to 2050

    Returns:
        pd.DataFrame: consumption data with efficiency enhancement factors applied
            Index: regional_id
            MultiIndex columns: 
                level=0: industry_sector
                level=1: application
    """
    logger.info(f"TODOD: ceheck ('2', 'Mechanische Energie')für 2030, cts, gas-> meine 0-en ausspuckt aber bei seinem code werte")


    # 0. validate the input
    if sector not in ['cts', 'industry']:
        raise ValueError("`sector` must be 'cts' or 'industry'")
    if energy_carrier not in ['power', 'gas', 'petrol']:
        raise ValueError("`energy_carrier` must be 'power', 'gas' or 'petrol'")
    

    # 1. load from cache if not force_preprocessing and cache exists
    if not force_preprocessing:
         consumption_data_with_efficiency_factor = load_consumption_data_with_efficiency_factor_cache(sector=sector, energy_carrier=energy_carrier, year=year)

         if consumption_data_with_efficiency_factor is not None:
            logger.info(f"Loading from cache: disagg_applications_efficiency_factor(sector={sector}, energy_carrier={energy_carrier}, year={year})")
            return consumption_data_with_efficiency_factor

    
    # 2. get consumption data dissaggregated by industry sector and regional_id for a year and energy carrier[power, gas, petrol]
    consumption_data_sectors_regional = get_consumption_data(year=year, energy_carrier=energy_carrier, force_preprocessing=force_preprocessing)


    # 3. filter for relevant sector:
    sectors_industry_sectors = dict_cts_or_industry_per_industry_sector()[sector]
    consumption_data_sectors_regional = consumption_data_sectors_regional.loc[consumption_data_sectors_regional.index.intersection(sectors_industry_sectors)]


    # 4. dissaggregate for applications - cosnumption data is already fiilterd to contain only relevant industry_sectors(cts/industry)
    consumption_data_dissaggregated = dissaggregate_for_applications(consumption_data=consumption_data_sectors_regional, year=year, sector=sector, energy_carrier=energy_carrier)



    # 5. apply efficiency effect
    consumption_data_with_efficiency_factor = apply_efficiency_factor(consumption_data=consumption_data_dissaggregated, sector=sector, energy_carrier=energy_carrier, year=year)


    # 6. save to cache
    logger.info("Saving to cache...")
    processed_dir = load_config("base_config.yaml")['consumption_data_with_efficiency_factor_cache_dir']
    processed_file = os.path.join(processed_dir, load_config("base_config.yaml")['consumption_data_with_efficiency_factor_cache_file'].format(sector=sector, energy_carrier=energy_carrier, year=year))
    os.makedirs(processed_dir, exist_ok=True)
    consumption_data_with_efficiency_factor.to_csv(processed_file)
    logger.info(f"Cached: disagg_applications_efficiency_factor(sector={sector}, energy_carrier={energy_carrier}, year={year})")
       

    return consumption_data_with_efficiency_factor



