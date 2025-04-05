import os
import pandas as pd
from src import logger
from src.configs.config_loader import load_config

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
            - columns: ['Beleuchtung', 'IKT', 'Klimakälte', 'Prozesskälte', 'Mechanische Energie', 'Prozesswärme', 'Raumwärme', 'Warmwasser',
                        'Strom Netzbezug', 'Strom Eigenerzeugung']
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
    df_decom_power['Strom Eigenerzeugung'].fillna(0, inplace=True)
    # Then fill any remaining missing values with 1
    df_decom_power.fillna(1, inplace=True)

    # rename the columns
    df_decom_power = df_decom_power.rename(columns={
        'Beleuchtung': 'lighting',
        'IKT': 'information_communication_technology',
        'Klimakälte': 'space_cooling',
        'Prozesskälte': 'process_cooling',
        'Mechanische Energie': 'mechanical_energy',
        'Prozesswärme': 'process_heat',
        'Raumwärme': 'space_heating',
        'Warmwasser': 'hot_water',
        'Strom Netzbezug': 'electricity_grid',
        'Strom Eigenerzeugung': 'electricity_self_generation'
    })
    
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
    df_decom_gas = df_decom_gas.rename(columns={
        'Anteil Erdgas am Verbrauch aller Gase': 'share_natural_gas_total_gas',
        'Energetischer Erdgasverbrauch': 'natural_gas_consumption_energetic',
        'Nichtenergetische Nutzung': 'non_energetic_use',
        'Mechanische Energie': 'mechanical_energy',
        'Prozesswärme': 'process_heat',
        'Raumwärme': 'space_heating',
        'Warmwasser': 'hot_water'
    })
        

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
    
    # rename the columns
    df_decom_temp_industry = df_decom_temp_industry.rename(columns={
        'Prozesswärme <100°C': 'process_heat_below_100C',
        'Prozesswärme 100°C-200°C': 'process_heat_100_to_200C',
        'Prozesswärme 200°C-500°C': 'process_heat_200_to_500C',
        'Prozesswärme >500°C': 'process_heat_above_500C'
    })
    
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
        raise FileNotFoundError(f"Factor gas no selfgen cache file for year {year} not found. Run consumption.calculate_self_generation() first.")
    file = pd.read_csv(cache_file.format(year=year))
    file.set_index('industry_sector', inplace=True)

    return file

# efficiency rate
def load_efficiency_rate(sector: str, source: str) -> pd.DataFrame:
    """
    Load the efficiency enhancement rate DataFrame based on sector and source.
    Returns a DataFrame with either 'until year' or 'WZ' as index.
    """
    file_path = 'data/raw/temporal/Efficiency_Enhancement_Rates_Applications.xlsx'

    sheet_map = {
        ("CTS", "power"): "eff_enhance_el_cts",
        ("CTS", "gas"): "eff_enhance_gas_cts",
        ("industry", "power"): "eff_enhance_industry",
        ("industry", "gas"): "eff_enhance_industry"
    }
    sheet_name = sheet_map.get((sector, source), "eff_enhance_industry")

    df = pd.read_excel(file_path, sheet_name=sheet_name)

    if sector == "CTS":
        return df.set_index("until year")
    else:
        return df.set_index("WZ").transpose()