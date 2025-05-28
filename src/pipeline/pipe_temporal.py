import pandas as pd

from src.data_processing.temporal import *
from src.data_processing.application import *





def disaggregate_temporal(energy_carrier: str, sector: str, year: int, force_preprocessing: bool = False, float_precision: int = 10) -> pd.DataFrame:
    """
    Disaggregate the temporal data for a given energy carrier and sector.


    Args:
        energy_carrier (str): The energy carrier to disaggregate.
        sector (str): The sector to disaggregate.
        year (int): The year to disaggregate.
        force_preprocessing (bool, optional): Whether to force the preprocessing. Defaults to False.
        float_precision (int, optional): The precision of the float to reduce the file size. Defaults to 10.

    Returns:
        pd.DataFrame: 
            MultiIndex columns: [regional_id, industry_sector]
            index: hours/15min of the given year
        e.g. for cts gas: 8760 rows x 23200 (=400*58) columns = 2.032.3200  values -> 3.59GB with full float precision 18
    """

    # 0. validate the input
    if sector not in ['cts', 'industry']:
        raise ValueError("`sector` must be 'cts' or 'industry'")
    if energy_carrier not in ['power', 'gas', 'petrol']:
        raise ValueError("`energy_carrier` must be 'power', 'gas' or 'petrol'")
    if year < 2000 or year > 2045:
        raise ValueError("`year` must be between 2015 and 2050")


    # 0.1 check if the cache exists
    if not force_preprocessing:
        consumption_disaggregate_temporal = load_consumption_disaggregate_temporal_cache(sector=sector, energy_carrier=energy_carrier, year=year)

        if consumption_disaggregate_temporal is not None:
            logger.info(f"Loading from cache: disaggregate_temporal(sector={sector}, energy_carrier={energy_carrier}, year={year})")
            return consumption_disaggregate_temporal


    # 1. Get the consumption data with efficiency factor
    consumption_data = disagg_applications_efficiency_factor(sector=sector, energy_carrier=energy_carrier, year=year, force_preprocessing=force_preprocessing)



    # 2. disaggregate the consumption data based on the energy carrier and sector
    if sector == "industry":
        # sum over the applications but with efficiency factor
        consumption_data = consumption_data.T.groupby(level=0).sum().T
        consumption_disaggregate_temporal = disaggregate_temporal_industry(consumption_data=consumption_data, year=year, low=0.5, force_preprocessing=force_preprocessing)

    elif sector == "cts":
        if energy_carrier == "gas":
             # sum over the applications but with efficiency factor
            consumption_data = consumption_data.T.groupby(level=0).sum().T
            consumption_disaggregate_temporal = disagg_temporal_heat_CTS(consumption_data=consumption_data, year=year)

        elif energy_carrier == "power":
             # sum over the applications but with efficiency factor
            consumption_data = consumption_data.T.groupby(level=0).sum().T
            consumption_disaggregate_temporal = disaggregate_temporal_power_CTS(consumption_data=consumption_data, year=year)

        elif energy_carrier == "petrol":

            # resolve with SLPs
            consumption_data = consumption_data.T.groupby(level=0).sum().T
            consumption_disaggregate_temporal = disagg_temporal_petrol_CTS(consumption_data=consumption_data, year=year)

        

    # sanity check
    if not np.isclose(consumption_disaggregate_temporal.sum().sum(), consumption_data.sum().sum(), atol=1e-6):
        raise ValueError(f"The sum of the disaggregated temporal consumption is not equal to the sum of the initial consumption data for {sector} and {energy_carrier} in year {year}")
    if consumption_disaggregate_temporal.isna().any().any():
        raise ValueError(f"The disaggregated temporal consumption contains NaN values for {sector} and {energy_carrier} in year {year}")


    # 2. save to cache
    logger.info("Saving to cache...")
    processed_dir = load_config("base_config.yaml")['consumption_disaggregate_temporal_cache_dir']
    processed_file = os.path.join(processed_dir, load_config("base_config.yaml")['consumption_disaggregate_temporal_cache_file'].format(energy_carrier=energy_carrier, year=year, sector=sector))
    os.makedirs(processed_dir, exist_ok=True)
    consumption_disaggregate_temporal.to_csv(processed_file, float_format=f"%.{float_precision}f")
    logger.info(f"Disaggregated temporal consumption for {sector} and {energy_carrier} in year {year} saved to {processed_file}")


    return consumption_disaggregate_temporal


def disaggregate_temporal_applications(energy_carrier: str, sector: str, year: int, force_preprocessing: bool = False, float_precision: int = 10) -> pd.DataFrame:
    """
    Disaggregate the temporal data for a given energy carrier and sector.


    Args:
        energy_carrier (str): The energy carrier to disaggregate.
        sector (str): The sector to disaggregate.
        year (int): The year to disaggregate.
        force_preprocessing (bool, optional): Whether to force the preprocessing. Defaults to False.
        float_precision (int, optional): The precision of the float to reduce the file size. Defaults to 10.

    Returns:
        pd.DataFrame: 
            MultiIndex columns: [regional_id, industry_sector]
            index: hours/15min of the given year
        e.g. for cts gas: 8760 rows x 23200 (=400*58) columns = 2.032.3200  values -> 3.59GB with full float precision 18
    """

    # 0. validate the input
    if sector not in ['cts', 'industry']:
        raise ValueError("`sector` must be 'cts' or 'industry'")
    if energy_carrier not in ['power', 'gas', 'petrol']:
        raise ValueError("`energy_carrier` must be 'power', 'gas' or 'petrol'")
    if year < 2000 or year > 2045:
        raise ValueError("`year` must be between 2015 and 2050")


    # 0.1 check if the cache exists
    if not force_preprocessing:
        consumption_disaggregate_temporal = load_consumption_disaggregate_temporal_cache(sector=sector, energy_carrier=energy_carrier, year=year)

        if consumption_disaggregate_temporal is not None:
            logger.info(f"Loading from cache: disaggregate_temporal(sector={sector}, energy_carrier={energy_carrier}, year={year})")
            return consumption_disaggregate_temporal


    # 1. Get the consumption data with efficiency factor
    consumption_data = disagg_applications_efficiency_factor(sector=sector, energy_carrier=energy_carrier, year=year, force_preprocessing=force_preprocessing)



    # 2. disaggregate the consumption data based on the energy carrier and sector
    # for faster execution speed we aggregate the applications before disaggregation
    if sector == "industry":
        # sum over the applications but with efficiency factor
        consumption_data = consumption_data.T.groupby(level=0).sum().T
        consumption_disaggregate_temporal = disaggregate_temporal_industry(consumption_data=consumption_data, year=year, low=0.5, force_preprocessing=force_preprocessing)

    elif sector == "cts":
        if energy_carrier == "gas":
             # sum over the applications but with efficiency factor
            consumption_data = consumption_data.T.groupby(level=0).sum().T
            consumption_disaggregate_temporal = disagg_temporal_heat_CTS(consumption_data=consumption_data, year=year)

        elif energy_carrier == "power":
             # sum over the applications but with efficiency factor
            consumption_data = consumption_data.T.groupby(level=0).sum().T
            consumption_disaggregate_temporal = disaggregate_temporal_power_CTS(consumption_data=consumption_data, year=year)

        elif energy_carrier == "petrol":

            # resolve with SLPs
            consumption_data = consumption_data.T.groupby(level=0).sum().T
            consumption_disaggregate_temporal = disagg_temporal_petrol_CTS(consumption_data=consumption_data, year=year)

        

    # 3. disaggregate for applications
    consumption_disaggregate_temporal = disaggregate_for_applications(consumption_data=consumption_data, year=year, sector=sector, energy_carrier=energy_carrier)




    # sanity check
    if not np.isclose(consumption_disaggregate_temporal.sum().sum(), consumption_data.sum().sum(), atol=1e-6):
        raise ValueError(f"The sum of the disaggregated temporal consumption is not equal to the sum of the initial consumption data for {sector} and {energy_carrier} in year {year}")
    if consumption_disaggregate_temporal.isna().any().any():
        raise ValueError(f"The disaggregated temporal consumption contains NaN values for {sector} and {energy_carrier} in year {year}")


    # 2. save to cache
    logger.info("Saving to cache...")
    processed_dir = load_config("base_config.yaml")['consumption_disaggregate_temporal_cache_dir']
    processed_file = os.path.join(processed_dir, load_config("base_config.yaml")['consumption_disaggregate_temporal_cache_file'].format(energy_carrier=energy_carrier, year=year, sector=sector))
    os.makedirs(processed_dir, exist_ok=True)
    consumption_disaggregate_temporal.to_csv(processed_file, float_format=f"%.{float_precision}f")
    logger.info(f"Disaggregated temporal consumption for {sector} and {energy_carrier} in year {year} saved to {processed_file}")


    return consumption_disaggregate_temporal





