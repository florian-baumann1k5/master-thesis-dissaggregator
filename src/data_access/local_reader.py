import os
import pandas as pd
from src import logger
from src.configs.config_loader import load_config


def load_preprocessed_ugr_file_if_exists(year: int, force_preprocessing: bool) -> pd.DataFrame | None:
    processed_dir = load_config("base_config.yaml")['base_year']
    processed_file = os.path.join(processed_dir, f"ugr_preprocessed_{year}.csv")

    if not force_preprocessing and os.path.exists(processed_file):
        return pd.read_csv(processed_file, index_col="industry_sector")
    return None

def load_raw_ugr_data() -> pd.DataFrame:
    raw_file = "data/raw/dimensionless/ugr_2000to2020.csv"
    return pd.read_csv(raw_file, delimiter=';')

def load_genisis_wz_sector_mapping_file() -> pd.DataFrame:
    mapping_file = "src/configs/genisis_wz_dict.csv"
    return pd.read_csv(mapping_file)




