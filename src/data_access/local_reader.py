import os
import pandas as pd
import numpy as np
from netCDF4 import Dataset

from src import logger
from src.configs.config_loader import load_config
from src.utils.utils import translate_application_columns, fix_region_id


# UGR data
def load_preprocessed_ugr_file_if_exists(year: int, force_preprocessing: bool) -> pd.DataFrame | None:
    preprocessed_dir = load_config("base_config.yaml")['preprocessed_dir']
    preprocessed_file = os.path.join(preprocessed_dir, f"ugr_preprocessed_{year}.csv")

    if not force_preprocessing and os.path.exists(preprocessed_file):
        return pd.read_csv(preprocessed_file, index_col="industry_sector")
    return None

def load_raw_ugr_data() -> pd.DataFrame:
    raw_file = load_config("base_config.yaml")['ugr_genisis_data_file'] 
    return pd.read_csv(raw_file, delimiter=';')

def load_genisis_wz_sector_mapping_file() -> pd.DataFrame: #TODO low: make this a config function
    raw_file = "src/configs/genisis_wz_dict.csv"
    return pd.read_csv(raw_file)


# Activity drivers
def load_activity_driver_employees() -> pd.DataFrame:
    raw_file = "data/raw/temporal/Activity_drivers.xlsx"

    #TODO: make this a csv file
    df_driver_industry = pd.read_excel(raw_file, sheet_name=("drivers_industry_emp"), skiprows=1).set_index('year')
    df_driver_cts = pd.read_excel(raw_file, sheet_name=("drivers_cts_emp"), skiprows=1).set_index('year')
    emp_total = df_driver_industry.join(df_driver_cts)
    # normalize projection using last available year from database (2030)
    emp_total = emp_total.apply(lambda x: x/x.loc[2030])

    return emp_total

def load_activity_driver_consumption() -> pd.DataFrame:
    """
    DISS 4.5
    activity drivers for consumption for the years 2015-2050 and the 87 industry_sectors (no 35)
    """
    raw_file = "data/raw/temporal/Activity_drivers.xlsx"

    #TODO: make this a csv file
    drivers_industry_gva = pd.read_excel(raw_file, sheet_name=("drivers_industry_gva"), skiprows=1).set_index('year')
    drivers_cts_area = pd.read_excel(raw_file, sheet_name=("drivers_cts_area"), skiprows=1).set_index('year')
    drivers_total = drivers_industry_gva.join(drivers_cts_area).fillna(0.0)


    return drivers_total


# Decomposition factors
def load_decomposition_factors_power() -> pd.DataFrame:
    """
    Load the decomposition factors for the energy carriers power.

    Returns:
        pd.DataFrame:
            - index:   industry_sectors (88 unique industry_sectors)
            - columns: [<applications>]
    """
    # File path (using your helper function)
    file_path = "data/raw/dimensionless/decomposition_factors.xlsx"
    sheets = pd.read_excel(file_path, sheet_name=None)
    
    # Extract the needed sheets
    df_decom_power = sheets['Endenergieverbrauch Strom']
    
    # Set 'WZ' as the index for both DataFrames
    df_decom_power.set_index('WZ', inplace=True)
    
    # rename the index to industry_sectors
    df_decom_power.index.name = 'industry_sectors'
        
    # Fill missing values for industry_sector 35:
    # First, for 'Strom Eigenerzeugung' missing values become 0
    df_decom_power['Strom Eigenerzeugung'] = df_decom_power['Strom Eigenerzeugung'].fillna(0)
    # Then fill any remaining missing values with 1
    df_decom_power.fillna(1, inplace=True)

    # translate the columns
    df_decom_power = translate_application_columns(df_decom_power)
    
    return df_decom_power

def load_decomposition_factors_gas() -> pd.DataFrame:
    """
    Load the decomposition factors for the energy carriers gas.

    Returns:
        pd.DataFrame:
            - index:   industry_sectors (88 unique industry_sectors)
            - columns: ['Anteil Erdgas am Verbrauch aller Gase', 'Energetischer Erdgasverbrauch', 
                        'Nichtenergetische Nutzung', 'Mechanische Energie', 'Prozesswärme', 'Raumwärme', 'Warmwasser']
    """

    # File path (using your helper function)
    file_path = "data/raw/dimensionless/decomposition_factors.xlsx"
    sheets = pd.read_excel(file_path, sheet_name=None)
    
    # Extract the needed sheets
    df_decom_gas = sheets['Endenergieverbrauch Gas']
    
    # Set 'WZ' as the index for both DataFrames
    df_decom_gas.set_index('WZ', inplace=True)

    # Fill missing values for industry_sector 35:
    df_decom_gas.fillna(1, inplace=True)
    
    # rename the index to industry_sectors
    df_decom_gas.index.name = 'industry_sectors'

    # rename the columns
    df_decom_gas = translate_application_columns(df_decom_gas)
        

    return df_decom_gas

def load_decomposition_factors_temperature_industry() -> pd.DataFrame:
    """
    Load the decomposition factors for the temperature industry.

    Returns:
        pd.DataFrame:
            - index:   industry_sectors industry (5-33 unique industry_sectors)
            - columns: 'Prozesswärme <100°C', 'Prozesswärme 100°C-200°C', 'Prozesswärme 200°C-500°C', 'Prozesswärme >500°C'
    """
    # File path (using your helper function)
    file_path = "data/raw/dimensionless/decomposition_factors.xlsx"
    sheets = pd.read_excel(file_path, sheet_name=None)
    
    # Extract the needed sheets
    df_decom_temp_industry = sheets['Prozesswärme_Temperaturniveaus']
    
    # Set 'WZ' as the index for both DataFrames
    df_decom_temp_industry.set_index('WZ', inplace=True)

    # rename the index to industry_sectors
    df_decom_temp_industry.index.name = 'industry_sectors'

    # translate the columns
    df_decom_temp_industry = translate_application_columns(df_decom_temp_industry)
    
    return df_decom_temp_industry


# gas self consumption
def load_gas_industry_self_consuption(year: int) -> pd.DataFrame:
    """
    Retuns the bilanz<year>d.xlsx file. For the years 2007-2019, it returns the sheet "nat" (="Natürliche Einheiten").
    Files are stored in data/raw/dimensionless/energiebilanz/
    """

    if not (2007 <= year <= 2019):
        raise ValueError("Data for gas industry self consumption is only available for the years 2007-2019. You requested data for the year " + str(year))


    raw_file = "data/raw/dimensionless/energiebilanz/bilanz" + str(year)[-2:] + "d.xlsx"
    return pd.read_excel(raw_file, sheet_name="nat", skiprows=3)

def load_gas_industry_self_consuption_cache() -> pd.DataFrame:
    cache_file = load_config("base_config.yaml")['gas_industry_self_consumption_cache_file']
    file = pd.read_csv(cache_file)

    return file

def load_factor_gas_no_selfgen_cache(year: int) -> pd.DataFrame:
    """
    Loads the factor_gas_no_selfgen_cache file for the given year. 
    Calculated and cached in consumption.calculate_self_generation()

    Returns:
        pd.DataFrame:
            - index: industry_sectors
            - columns: factor_gas_no_selfgen
    """
    cache_file = load_config("base_config.yaml")['factor_gas_no_selfgen_cache_file']
    if not os.path.exists(cache_file.format(year=year)):
        raise FileNotFoundError(f"Factor gas no selfgen cache file {cache_file.format(year=year)} not found. Run consumption.calculate_self_generation() via get_consumption_data_historical_and_future() first.")
    file = pd.read_csv(cache_file.format(year=year))
    file.set_index('industry_sector', inplace=True)

    return file


# Efficiency rate
def load_efficiency_rate(sector: str, energy_carrier: str) -> pd.DataFrame: 

    """
    Load the efficiency enhancement rate DataFrame based on sector and energy_carrier.
    Returns a DataFrame with either 'until year' or 'WZ' as index.
    """
    file_path = 'data/raw/temporal/Efficiency_Enhancement_Rates_Applications.xlsx'

    sheet_map = {
        ("cts", "power"): "eff_enhance_el_cts",
        ("cts", "gas"): "eff_enhance_gas_cts",
        ("industry", "power"): "eff_enhance_industry",
        ("industry", "gas"): "eff_enhance_industry"
    }
    sheet_name = sheet_map.get((sector, energy_carrier), "eff_enhance_industry") # arg2 is default value

    df = pd.read_excel(file_path, sheet_name=sheet_name)

    if sector == "cts":
        df = df.set_index("until year")
        df.index.name = "until_year"
    else:
        df = df.set_index("WZ")
        df.index.name = "industry_sector"
        df = df.transpose()

    """ returns:
    cts power/gas: eff_enhance_industry
    WZ       5      6      7      8      9      10     11     12     13     14     15     16  ...     22     23      24     25     26     27     28     29     30     31     32     33
    2035  0.019  0.019  0.019  0.019  0.019  0.019  0.019  0.019  0.019  0.019  0.019  0.019  ...  0.019  0.005  0.0025  0.019  0.019  0.019  0.019  0.019  0.019  0.019  0.019  0.019
    2045  0.013  0.013  0.013  0.013  0.013  0.013  0.013  0.013  0.013  0.013  0.013  0.013  ...  0.013  0.005  0.0050  0.013  0.013  0.013  0.013  0.013  0.013  0.013  0.013  0.013
    [2 rows x 29 columns]

    cts gas : eff_enhance_gas_cts
                Mechanische Energie  Prozesswärme  Raumwärme  Warmwasser  Nichtenergetische Nutzung
    until year                                                                                     
    2035                     0.0150        0.0130     0.0250      0.0250                          0
    2050                     0.0075        0.0065     0.0125      0.0125                          0
    [2 rows x 5 columns]

    cts power: eff_enhance_el_cts
                Beleuchtung     IKT  Klimakälte  Prozesskälte  Mechanische Energie  Prozesswärme  Raumwärme  Warmwasser  Nichtenergetische Nutzung
    until year                                                                                                                                    
    2035             0.0210  0.0070      -0.005        0.0330               0.0150        0.0130     0.0090      0.0090                          0
    2050             0.0105  0.0035      -0.005        0.0165               0.0075        0.0065     0.0045      0.0045                          0
    [2 rows x 9 columns]
    """

    # rename the columns to english standard
    column_rename_map = {
        'Anteil Erdgas am Verbrauch aller Gase': 'share_natural_gas_total_gas',
        'Energetischer Erdgasverbrauch': 'natural_gas_consumption_energetic',
        'Nichtenergetische Nutzung': 'non_energetic_use',
        'Mechanische Energie': 'mechanical_energy',
        'Prozesswärme': 'process_heat',
        'Raumwärme': 'space_heating',
        'Warmwasser': 'hot_water',
        'Beleuchtung': 'lighting',
        'IKT': 'information_communication_technology',
        'Klimakälte': 'space_cooling',
        'Prozesskälte': 'process_cooling',
        'Strom Netzbezug': 'electricity_grid',
        'Strom Eigenerzeugung': 'electricity_self_generation'
    }
    # Only rename columns that exist in the DataFrame
    df = df.rename(columns={k: v for k, v in column_rename_map.items() if k in df.columns})

    return df


# Load profiles
def load_power_load_profile(profile: str) -> pd.DataFrame:
    """
    Retuns the power load profiles for the given profile.
    DISS: "4.2.5.2 Standardlastprofile" -> Tabelle A.9
    """

    raw_file = f"data/raw/temporal/power_load_profiles/39_VDEW_Strom_Repräsentative_Profile_{profile}.xlsx"
    load_profiles = pd.read_excel(raw_file)

    return load_profiles

def load_gas_load_profile(profile: str) -> pd.DataFrame:
    """
    Loads the gas shift load profile for the given profile/slp.
    """

    raw_file = f"data/raw/temporal/gas_load_profiles/Lastprofil_{profile}.xls"
    load_profiles = pd.read_excel(raw_file)

    return load_profiles

def load_shift_load_profiles_by_year_cache(year: int) -> pd.DataFrame:
    """
    Loads the shift load profiles for the given year. 
    Returns a Multicolumn dataframe: [state, shift_load_profile]
    """
    cache_dir = load_config("base_config.yaml")['shift_load_profiles_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['shift_load_profiles_cache_file'].format(year=year))

    if not os.path.exists(cache_file):
        return None
    file = pd.read_csv(cache_file, header=[0, 1], index_col=0)
    return file




# Temperature
def load_temperature_allocation_cache(year: int) -> pd.DataFrame:
    """
    Loads the temperature allocation cache for the given year.
    Returns:
        pd.DataFrame:
            - index: regional_id
            - columns: temperature per day for a given year
        if not exists, returns None
    """
    cache_dir = load_config("base_config.yaml")['temperature_allocation_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['temperature_allocation_cache_file'].format(year=year))

    if not os.path.exists(cache_file):
        return None
    file = pd.read_csv(cache_file, index_col=0)
    return file

def load_disagg_daily_gas_slp_cts_cache(state: str, year: int) -> pd.DataFrame:
    """
    Loads the disaggregated daily gas shift load profiles for the given state and year.

    Returns:
        pd.DataFrame:
            MultiIndex columns: [regional_id, industry_sector]
            index: days of the year
    """
    cache_dir = load_config("base_config.yaml")['disagg_daily_gas_slp_cts_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['disagg_daily_gas_slp_cts_cache_file'].format(state=state, year=year))

    if not os.path.exists(cache_file):
        return None
    file = pd.read_csv(cache_file, header=[0, 1], index_col=0)
    return file

    
# Heat
def load_fuel_switch_share(sector: str, switch_to: str) -> pd.DataFrame:
    """
    Loads the fuel switch share for the given sector and switch_to[power, hydrogen] in the year 2045.

    Args:
        sector: str
        switch_to: str

    Returns:
        pd.DataFrame:
            - index: WZ
    """

    SHEET = {
        "cts": {
            "power": "Gas2Power CTS 2045",
        },
        "industry": {
            "power":    "Gas2Power industry 2045",
            "hydrogen": "Gas2Hydrogen industry 2045",
            "electrode": "Gas2Power industry electrode",
        }
    }
    PATH = 'data/raw/heat/fuel_switch_keys.xlsx'


    try:
        sheet_name = SHEET[sector][switch_to]
    except KeyError:
        raise ValueError(
            f"`switch_to` must be one of {list(SHEET[sector])} for '{sector}'"
        )

    df = pd.read_excel(PATH, sheet_name=sheet_name, skiprows=1)
    
    df = translate_application_columns(df)

    return df

def load_shift_load_profiles_by_year_cache(year: int) -> pd.DataFrame:
    """
    Loads the shift load profiles for the given year. 
    Returns a Multicolumn dataframe: [state, shift_load_profile]
    """
    cache_dir = load_config("base_config.yaml")['shift_load_profiles_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['shift_load_profiles_cache_file'].format(year=year))

    if not os.path.exists(cache_file):
        return None
    file = pd.read_csv(cache_file, header=[0, 1], index_col=0)
    return file

def load_ERA_temperature_data(year: int) -> pd.DataFrame:
    """
    Loads the ERA temperature data for the given year.
    """
    cache_dir = load_config("base_config.yaml")['era_temperature_data_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['era_temperature_data_cache_file'].format(year=year))

    try:
        file = Dataset(cache_file, only_use_cftime_datetimes=False,  only_use_python_datetimes=True)
    except FileNotFoundError:
        raise FileNotFoundError(f"ERA temperature data for year {year} not found. File not found: {cache_file}")
        return None
    
    return file

def load_cop_parameters() -> pd.DataFrame:

    filepath = "data/raw/heat/cop_parameters.csv"
    try:    
        data = pd.read_csv(filepath, sep=';', decimal=',', header=0, index_col=0)
        data.apply(pd.to_numeric, downcast='float')
    except FileNotFoundError:
        raise FileNotFoundError(f"COP parameters file not found: {filepath}")
    
    return data





# Temperature
def load_temperature_allocation_cache(year: int, resolution: str) -> pd.DataFrame:
    """
    Loads the temperature allocation cache for the given year.
    Returns:
        pd.DataFrame:
            - index: regional_id
            - columns: temperature per day for a given year
        if not exists, returns None
    """
    cache_dir = load_config("base_config.yaml")['temperature_allocation_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['temperature_allocation_cache_file'].format(year=year, resolution=resolution))

    if not os.path.exists(cache_file):
        return None
    file = pd.read_csv(cache_file, index_col=0)
    return file

def load_disagg_daily_gas_slp_cts_cache(state: str, year: int) -> pd.DataFrame:
    """
    Loads the disaggregated daily gas shift load profiles for the given state and year.

    Returns:
        pd.DataFrame:
            MultiIndex columns: [regional_id, industry_sector]
            index: days of the year
    """
    cache_dir = load_config("base_config.yaml")['disagg_daily_gas_slp_cts_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['disagg_daily_gas_slp_cts_cache_file'].format(state=state, year=year))

    if not os.path.exists(cache_file):
        return None
    file = pd.read_csv(cache_file, header=[0, 1], index_col=0)
    return file

    

# Pipeline caches
def load_consumption_data_cache(year: int, energy_carrier: str) -> pd.DataFrame:
    """
    Loads the consumption data cache for the given year and energy carrier.
    """
    cache_dir = load_config("base_config.yaml")['consumption_data_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['consumption_data_cache_file'].format(energy_carrier=energy_carrier, year=year))

    if not os.path.exists(cache_file):
        return None
    file = pd.read_csv(cache_file, index_col="industry_sector")

    return file

def load_consumption_data_with_efficiency_factor_cache(sector: str, energy_carrier: str, year: int) -> pd.DataFrame:
    """
    Loads the consumption data cache with efficiency factor for the given sector and energy carrier.
    """
    cache_dir = load_config("base_config.yaml")['consumption_data_with_efficiency_factor_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['consumption_data_with_efficiency_factor_cache_file'].format(sector=sector, energy_carrier=energy_carrier, year=year))

    if not os.path.exists(cache_file):
        return None
    file = pd.read_csv(cache_file, header=[0, 1], index_col=0)

    return file

def load_consumption_disaggregate_temporal_cache(sector: str, energy_carrier: str, year: int) -> pd.DataFrame:
    """
    Loads the consumption data cache with efficiency factor for the given sector and energy carrier.

    Returns:
        pd.DataFrame:
            MultiIndex columns: [regional_id, industry_sector]
            index: days of the year
    """
    cache_dir = load_config("base_config.yaml")['consumption_disaggregate_temporal_cache_dir']
    cache_file = os.path.join(cache_dir, load_config("base_config.yaml")['consumption_disaggregate_temporal_cache_file'].format(sector=sector, energy_carrier=energy_carrier, year=year))

    if not os.path.exists(cache_file):
        return None
    file = pd.read_csv(cache_file, header=[0, 1], index_col=0)



# Others
def get_all_regional_ids() -> pd.DataFrame:
    """
    Returns all regional ids for the given year.
    """
    raw_file = "data/raw/regional/ags_lk_changes/landkreise_2023.csv"
    return pd.read_csv(raw_file)

def load_shapefiles_by_regional_id() -> pd.DataFrame:
    """
    Loads the shapefiles for the given year.
    """

    import shapely.wkt as wkt  # needed for converting strings to MultiPolygons
    import geopandas as gpd

    raw_file = "data/raw/regional/nuts3_shapefile.csv"

    try:
        df = pd.read_csv(raw_file)
    except FileNotFoundError:
        return None

    # convert strings to MultiPolygons
    geom = [wkt.loads(mp_str) for mp_str in df.geom_as_text]
    df = (gpd.GeoDataFrame(df.drop('geom_as_text', axis=1),
                             crs='EPSG:25832', geometry=geom)
               .assign(regional_id=lambda x: x.id_ags.apply(fix_region_id))
               .set_index('regional_id').sort_index(axis=0))
    
    return df


# Electric Vehicles
def load_registered_electric_vehicles_by_regional_id(year: int) -> pd.DataFrame:
    """
    Loads the registered electric vehicles by regional id for the given year.
    From the Kraftfahrt-Bundesamt: https://www.kba.de/DE/Statistik/Produktkatalog/produkte/Fahrzeuge/fz1_b_uebersicht.html
    Normalised to 399 regional_ids

    Args:
        year: int

    Returns:
        pd.DataFrame:
            - index: regional_id (int)
            - columns: number_of_registered_evs (int)
    """
    raw_file = f"data/raw/electric_vehicles/registered_evs_by_regional_id/registered_evs_{year}.csv"

    if not os.path.exists(raw_file):
        raise FileNotFoundError(f"Registered electric vehicles by regional id for year {year} not found. File not found: {raw_file}")
    

    df = pd.read_csv(raw_file, dtype=str)

    # Remove the dot in 'number_of_registered_evs' and convert to int
    df['number_of_registered_evs'] = df['number_of_registered_evs'].str.replace('.', '', regex=False).astype(int)
    # Convert 'regional_id' to int
    df['regional_id'] = df['regional_id'].astype(int)

    # Set 'regional_id' as the index
    df.set_index('regional_id', inplace=True)
    return df

def load_avg_km_by_car() -> pd.DataFrame:
    """
    Loads the average km by car for 2003-2023 in germany.
    Source: https://de.statista.com/statistik/daten/studie/251743/umfrage/durchschnittliche-fahrleistung-von-personenkraftwagen-in-deutschland/
    with data from the "Deutsche Automobil Treuhand (DAT) Report"s

    """
    raw_file = "data/raw/electric_vehicles/avg_km_by_car.csv"

    if not os.path.exists(raw_file):
        raise FileNotFoundError(f"Average km by car not found. File not found: {raw_file}")
    
    df = pd.read_csv(raw_file)

    # Convert to int
    df['year'] = df['year'].astype(int)
    df['avg_km_per_ev'] = df['avg_km_per_ev'].astype(int)

    df.set_index('year', inplace=True)

    
    return df

def load_future_ev_stock_s2() -> pd.DataFrame:
    """
    Loads the future ev stock szenarios for 2025-2045 in Mio.

    """
    raw_file = "data/raw/electric_vehicles/predicted_evs_s2.csv"

    df = pd.read_csv(raw_file)
    df['year'] = df['year'].astype(int)
    df.set_index('year', inplace=True)
    return df

def load_historical_vehicle_consumption_ugr_by_energy_carrier() -> pd.DataFrame:
    """
    Loads the historical ev stock for the given year.
    
    Source it the UGR Table "85521-15: Energieverbrauch im Straßenverkehr, Energieträger in tiefer Gliederung, Deutschland, 2014 bis 2022"

    Data needs to be preprocessed to be in the correct format and cannot be copied 1:1 from the UGR:
        - add seperator ";"
        - replace comma with dot for decimal conversion

    Args:
        -

    Returns:
        pd.DataFrame:
            - index: year
            - columns: ev_consumption (int)
    """
    raw_file = f"data/raw/electric_vehicles/ugr_85521-15_priv_households_energy_carrier_mobility.csv"

    if not os.path.exists(raw_file):
        raise FileNotFoundError(f"Historical ev stock not found. File not found: {raw_file}")
    

    df = pd.read_csv(raw_file, sep=';')

    return df


def load_ev_charging_profile(type: str, day_type: str) -> pd.DataFrame:
    """
    Loads the ev load profile for the given type.
    """
    raw_file = f"data/processed/electric_vehicles/ev_charging_profiles/ev_charging_profile_{type}_{day_type}.csv"

    if not os.path.exists(raw_file):
        raise FileNotFoundError(f"Ev charging profile not found. File not found: {raw_file}")
    
    df = pd.read_csv(raw_file, sep=';')
    df.set_index('time', inplace=True)
    return df

