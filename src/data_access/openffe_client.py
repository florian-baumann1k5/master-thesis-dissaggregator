import os
import json
import requests
import pandas as pd
from typing import Dict, Any, Optional
from src import logger

# Constants
BASE_URL = "https://api.opendata.ffe.de/"
CACHE_DIR = "data/api_cache/open_ffe"

# Ensure cache directory exists
os.makedirs(CACHE_DIR, exist_ok=True)

class OpenFFEApiError(Exception):
    """Custom exception for OpenFFE API errors."""
    pass

def generate_cache_filename(query: str) -> str:
    """
    Generate a readable cache filename based on the query.
    
    Args:
        query: The API query string
        
    Returns:
        A cache filename including the query
    """
    # Replace special characters with underscores to make it file system safe
    safe_query = query.replace("/", "_").replace("?", "_").replace("&", "_").replace("=", "_")
    
    # Truncate if too long (filesystem limits)
    if len(safe_query) > 200:
        safe_query = safe_query[:200]
        
    return f"{safe_query}.json"

def get_cache_path(query: str) -> str:
    """
    Get the full cache path for a query.
    
    Args:
        query: The API query string
        
    Returns:
        Full path to the cache file
    """
    filename = generate_cache_filename(query)
    return os.path.join(CACHE_DIR, filename)

def read_from_cache(query: str) -> Optional[Dict[str, Any]]:
    """
    Try to read response from cache.
    
    Args:
        query: The API query string
        
    Returns:
        Cached response data if available, None otherwise
    """
    cache_path = get_cache_path(query)
    
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                logger.info(f"Reading from cache: {cache_path}")
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Error reading cache file: {str(e)}")
            return None
    
    return None

def write_to_cache(query: str, data: Dict[str, Any]) -> None:
    """
    Write response data to cache.
    
    Args:
        query: The API query string
        data: Response data to cache
    """
    cache_path = get_cache_path(query)
    
    try:
        with open(cache_path, 'w') as f:
            json.dump(data, f, indent=2)
            logger.info(f"Wrote to cache: {cache_path}")
    except IOError as e:
        logger.error(f"Error writing to cache: {str(e)}")

def parse_response(response_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Parse API response data into a DataFrame.
    
    Args:
        response_data: JSON response data
        
    Returns:
        Parsed DataFrame
        
    Raises:
        OpenFFEApiError: If the response contains an error message
    """
    # Check if response contains an error message
    if 'message' in response_data and 'data' not in response_data:
        raise OpenFFEApiError(f"API returned error: {response_data['message']}")
    
    # Check if response has the expected format
    if 'data' not in response_data:
        logger.error("Response does not contain 'data' field")
        return pd.DataFrame()
    
    data = response_data['data']
    if not data or not isinstance(data, list):
        logger.warning("Response data is empty or not a list")
        return pd.DataFrame()
    
    # Extract all data
    df = pd.DataFrame(data)
    
    # Handle internal_id special case - expand the array into multiple columns
    if 'internal_id' in df.columns:
        # Find the maximum length of internal_id arrays
        max_id_length = 0
        for idx in df['internal_id']:
            if isinstance(idx, list) and len(idx) > max_id_length:
                max_id_length = len(idx)
        
        # Create new columns for each position in the internal_id array
        for i in range(max_id_length):
            column_name = f"internal_id[{i}]"
            
            # Extract the i-th element from each internal_id array, or None if not present
            df[column_name] = df['internal_id'].apply(
                lambda x: x[i] if isinstance(x, list) and i < len(x) else None
            )
        
        # Drop the original internal_id column
        df = df.drop('internal_id', axis=1)
    
    return df

def get_openffe_data(query: str, use_cache: bool = True) -> pd.DataFrame:
    """
    Make a GET request to the OpenFFE API.
    
    Args:
        query: The API endpoint query string
        use_cache: Whether to use cached responses
        
    Returns:
        DataFrame containing the API response data
        
    Raises:
        OpenFFEApiError: If the API returns an error message
        requests.RequestException: If the HTTP request fails
    """
    # Check cache first if enabled
    if use_cache:
        cached_data = read_from_cache(query)
        if cached_data:
            return parse_response(cached_data)
    
    # Make API request
    url = BASE_URL + query
    logger.info(f"Making API request to: {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for non-200 status codes
        
        data = response.json()
        
        # Try to parse the response first to check for API errors
        # If it contains an error message, parse_response will raise OpenFFEApiError
        df = parse_response(data)
        
        # Only cache if no error occurred
        if use_cache:
            write_to_cache(query, data)
        
        return df
        
    except requests.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise
