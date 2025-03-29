import os
import pandas as pd
from src import logger
from src.configs.config_loader import load_config


def load_preprocessed_ugr_file_if_exists(year: int, force_preprocessing: bool) -> pd.DataFrame | None:
    preprocessed_dir = load_config("base_config.yaml")['preprocessed_dir']
    preprocessed_file = os.path.join(preprocessed_dir, f"ugr_preprocessed_{year}.csv")

    if not force_preprocessing and os.path.exists(preprocessed_file):
        return pd.read_csv(preprocessed_file, index_col="industry_sector")
    return None

def load_raw_ugr_data() -> pd.DataFrame:
    raw_file = load_config("base_config.yaml")['ugr_genisis_data_file'] 
    return pd.read_csv(raw_file, delimiter=';')

def load_genisis_wz_sector_mapping_file() -> pd.DataFrame:
    raw_file = "src/configs/genisis_wz_dict.csv"
    return pd.read_csv(raw_file)


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

def load_decomposition_factors() -> pd.DataFrame:
    # File path (using your helper function)
    file_path = "data/raw/dimensionless/decomposition_factors.xlsx"
    sheets = pd.read_excel(file_path, sheet_name=None)
    
    # Extract the needed sheets
    df_decom_el = sheets['Endenergieverbrauch Strom']
    df_decom_gas = sheets['Endenergieverbrauch Gas']
    
    # Set 'WZ' as the index for both DataFrames
    df_decom_el.set_index('WZ', inplace=True)
    df_decom_gas.set_index('WZ', inplace=True)
    
    df_decom = pd.concat([
        df_decom_el[['Strom Netzbezug', 'Strom Eigenerzeugung']],
        df_decom_gas[['Anteil Erdgas am Verbrauch aller Gase']]
    ], axis=1)
        
    # Fill missing values:
    # First, for 'Strom Eigenerzeugung' missing values become 0
    df_decom['Strom Eigenerzeugung'].fillna(0, inplace=True)
    # Then fill any remaining missing values with 1
    df_decom.fillna(1, inplace=True)
    
    return df_decom

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
