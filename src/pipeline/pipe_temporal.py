import pandas as pd

from src.data_processing.temporal import *




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

    # 0. check cache
    if not force_preprocessing:
        consumption_disaggregate_temporal = load_consumption_disaggregate_temporal_cache(sector=sector, energy_carrier=energy_carrier, year=year)

        if consumption_disaggregate_temporal is not None:
            logger.info(f"Loading from cache: disaggregate_temporal(sector={sector}, energy_carrier={energy_carrier}, year={year})")
            return consumption_disaggregate_temporal


    # 1. Get the consumption data
    consumption_data = disagg_applications_efficiency_factor(sector=sector, energy_carrier=energy_carrier, year=year)
    consumption_data = consumption_data.T.groupby(level=0).sum().T


    if sector == "industry":
        consumption_disaggregate_temporal = disaggregate_temporal_industry(consumption_data=consumption_data, year=year, low=0.5)

        # TODO: aus temporal application
        # if energy_carrier == "gas":
            #if use_slp_for_sh:
            # ...

    elif sector == "cts":
        if energy_carrier == "gas":  
            consumption_disaggregate_temporal = disagg_temporal_gas_CTS(consumption_data=consumption_data, year=year)


        elif energy_carrier == "power":
            consumption_disaggregate_temporal = disaggregate_temporal_power_CTS(consumption_data=consumption_data, year=year)


    # 2. save to cache
    logger.info("Saving to cache...")
    processed_dir = load_config("base_config.yaml")['consumption_disaggregate_temporal_cache_dir']
    processed_file = os.path.join(processed_dir, load_config("base_config.yaml")['consumption_disaggregate_temporal_cache_file'].format(energy_carrier=energy_carrier, year=year, sector=sector))
    os.makedirs(processed_dir, exist_ok=True)
    consumption_disaggregate_temporal.to_csv(processed_file, float_format=f"%.{float_precision}f")
    logger.info(f"Disaggregated temporal consumption for {sector} and {energy_carrier} in year {year} saved to {processed_file}")


    return consumption_disaggregate_temporal








