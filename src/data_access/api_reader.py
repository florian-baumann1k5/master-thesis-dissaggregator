import pandas as pd
from src.data_access.openffe_client import get_openffe_data, OpenFFEApiError
from src import logger

def get_manufacturing_energy_consumption(year: int, spatial_id: int = 15, use_cache: bool = True) -> pd.DataFrame: # TODO: = data.get_regional_energy_consumption(year):

    """
    Get energy consumption data for manufacturing, mining and quarrying industries.
    
    Args:
        year: The year for which to fetch data
        spatial_id: The spatial ID to use (default: 15)
        use_cache: Whether to use cached responses if available
        
    Returns:
        DataFrame containing energy consumption data
        
    Raises:
        OpenFFEApiError: If no data is available for the specified year
        requests.RequestException: If the HTTP request fails
    """
    query = f"demandregio/demandregio_spatial?id_spatial={spatial_id}&year={year}"
    logger.info(f"Fetching manufacturing energy consumption for year {year}")
    
    try:
        return get_openffe_data(query, use_cache=use_cache)
    except OpenFFEApiError as e:
        logger.error(f"No data available for year {year}: {str(e)}")
        raise


def get_historical_employees(year: int, spatial_id: int = 18, use_cache: bool = True) -> pd.DataFrame:
    """
    Get historical employee data by WZ and regional code. 
    
    Args:
        year: The year for which to fetch data
        spatial_id: The spatial ID to use (default: 18)

    Returns:
        DataFrame containing energy consumption data. Columns:
        id_region       = regional code ( not normalized) 
        year            = year
        internal_id[0]  = branch code
        value           = employees
    
    """

    # check if the year is between 2000 and 2018, return the data of 2008 if it is between 2000 and 2008 ( no data availablefor 2000-2008) else raise an error
    if year < 2000 or year > 2018:
        raise ValueError(f"No historical employee data available for year {year}")
    elif year >= 2000 and year <= 2008:
        year = 2008

    # building the query
    query = f"demandregio/demandregio_spatial?id_spatial={spatial_id}&year={year}"
    logger.info(f"Fetching historical employee data for year {year}")
    
    try:
        df = get_openffe_data(query, use_cache=use_cache)
    except OpenFFEApiError as e:
        logger.error(f"No data available for year {year}: {str(e)}")
        raise

    return df

def get_future_employees(year: int, spatial_id: int = 27, use_cache: bool = True) -> pd.DataFrame:
    """
    Get future employee data by WZ and regional code.

    spatial_id = 27 -> Synthese: Soz. Beschäftigte je LK nach WZ und Jahr (2012..2035), Szenario Basis
    API:
        internal_id[1] = WZ2008 (05..33)
        id_region = regional code ( not normalized) 
        year = year
        value = number of employees
    
    Args:
        year: The year for which to fetch data
        spatial_id: The spatial ID to use (default: 18)

    Returns:
        DataFrame containing energy consumption data. Columns:
        id_region       = regional code ( not normalized) 
        year            = year
        internal_id[0]  = branch code
        value           = employees
    
    """

    # check if the year is between 2000 and 2018, return the data of 2008 if it is between 2000 and 2008 ( no data availablefor 2000-2008) else raise an error
    if year < 2018 or year > 2050:
        raise ValueError(f"No future employee data available for year {year}")
    elif year >= 2035:
        logger.info(f"No future employee data available for year {year}, using 2035 instead")
        year = 2035 # 2035 is the last year for which data is available

    # building the query
    query = f"demandregio/demandregio_spatial?id_spatial={spatial_id}&year={year}"
    logger.info(f"Fetching historical employee data for year {year}")
    
    try:
        df = get_openffe_data(query, use_cache=use_cache)
    except OpenFFEApiError as e:
        logger.error(f"No data available for year {year}: {str(e)}")
        raise

    return df

