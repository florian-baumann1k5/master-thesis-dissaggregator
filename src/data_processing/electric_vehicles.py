import pandas as pd
from src.data_access.local_reader import *



def calculate_electric_vehicle_consumption(data_in: float | pd.DataFrame, avg_km_per_ev: int, avg_mwh_per_km: float) -> pd.DataFrame:
    """
    Calculate the consumption of electric vehicles.
    TODO: description
    """


    # 1. calculate the total consumption per ev
    avg_consumption_per_ev_per_year_in_mwh = avg_km_per_ev * avg_mwh_per_km


    # 2. calculate the total consumption
    ev_consumption = avg_consumption_per_ev_per_year_in_mwh * data_in

    # 3. check if the data is a dataframe
    if isinstance(data_in, pd.DataFrame):
        # 3. rename the column in the dataframe
        ev_consumption.rename(columns={'number_of_registered_evs': 'ev_consumption[mwh]'}, inplace=True)

        # 4. make the ev_consumption[mwh] column float
        ev_consumption['ev_consumption[mwh]'] = ev_consumption['ev_consumption[mwh]'].astype(float)

        # 5. check for nan and 0 values
        if ev_consumption.isnull().any().any():
            raise ValueError("NaN values found in ev total consumption")
        if ev_consumption.eq(0).any().any():
            raise ValueError("0 values found in ev total consumption")
        
    
    return ev_consumption


def calculate_avg_km_by_car(year: int) -> int:
    """
    Calculate the average km by car for the given year.

    Datasource only contains the years 2003-2023.
    """

    # 1. find the year
    if year < 2003:
        year_in_dataset = 2003
    elif year > 2023:
        year_in_dataset = 2023
    else:
        year_in_dataset = year

    # 2. load data
    df = load_avg_km_by_car()

    # 3. get the data
    avg_km_by_car = df.loc[year_in_dataset, 'avg_km_per_ev']


    return avg_km_by_car


def calculate_avg_mwh_per_km() -> int:
    """
    Calculate the average mwh per km for the given year.

    Assumption from source [notion 30]: avg consumption is 0.21 kwh/km
    """

    # assumption from source: avg consumption is 0.21 kwh/km = 0.00021 mwh/km

    avg_mwh_per_km = 0.00021

    return avg_mwh_per_km


def registered_electric_vehicles_by_regional_id(year: int) -> pd.DataFrame:
    """
    Load the registered electric vehicles by regional id for the given year.

    Data from sourece is only available from the years 2017-2024

    Args:
        year: int
            Year to load the data for

    Returns:
        pd.DataFrame
            DataFrame with the registered electric vehicles by regional id
            Columns:
                - number_of_registered_evs: float
                    The number of registered electric vehicles
            Index:
                - regional_id: int
                    The regional id
    """

    # 1. load data
    if year < 2017:
        year_in_dataset = 2017
    elif year > 2024:
        year_in_dataset = 2024
    else:
        year_in_dataset = year


    df = load_registered_electric_vehicles_by_regional_id(year=year_in_dataset)
    return df


def calculate_existing_ev_stock(year: int) -> int:
    """
    Calculate the existing ev stock for the given year.
    """

    # 1. load data
    df = registered_electric_vehicles_by_regional_id(year=year)

    # 2. sum it and make it an integer  
    existing_ev_stock = int(df.sum())

    # 3. return the data
    return existing_ev_stock


def s1_future_ev_stock_15mio_by_2030(year,
             baseline_year=2025,
             baseline_ev=1.6e6,
             total_stock=49e6):
    """
    Estimate EV fleet size (absolute number of vehicles) in Germany for a given year
    between baseline_year and final_year, using piecewise linear interpolation.

    Assumptions:
        1. Political Goal of 15mio EVs by 2030
        2. Political Goal of zero CO2 emmisions by 2045
        3. All fossil fuel cars are transitioning to EVs by 2045
        4. Total car stock is constant at 49mio

    Args:
        year : int
            Year to estimate (must be between baseline_year and YEAR_FINAL inclusive).
        baseline_year : int
            Year corresponding to the baseline_ev value.
        baseline_ev : float
            Number of EVs (absolute) at baseline_year.
        total_stock : float
            Total number of vehicles (EV + non-EV), assumed constant.

    Returns:
        float
            Estimated number of EVs in the given year.
    """

    # 1. define the political goals:
    YEAR_Goal = 2030 
    EV_GOAL = 15e6
    YEAR_FINAL = 2045

    # 2. validate input
    if year < baseline_year or year > YEAR_FINAL:
        raise ValueError(f"Year must be between {baseline_year} and {YEAR_FINAL} (inclusive).")


    # 3. calculate the linear interpolation between today-2030 & 2031-2045
    if year <= YEAR_Goal:
        # interpolate between (baseline_year, baseline_ev) and (YEAR_Goal, EV_GOAL)
        return baseline_ev + (EV_GOAL - baseline_ev) * (year - baseline_year) / (YEAR_Goal - baseline_year)
    else:
        # interpolate between (YEAR_Goal, EV_GOAL) and (YEAR_FINAL, total_stock)
        return EV_GOAL + (total_stock - EV_GOAL) * (year - YEAR_Goal) / (YEAR_FINAL - YEAR_Goal)


def normalized_ev_distribution_by_region() -> pd.DataFrame:
    """
    Load the normalized number of electric vehicles distribution by region for the given year.

    Args:
        -

    Returns:
        pd.DataFrame
            DataFrame with the normalized number of electric vehicles distribution by region
            Columns:
                - regional_id: int
                    The regional id
                - ev_share: float
                    The share of electric vehicles in the region
    """
    year = load_config()["last_year_existing_registration_data"]

    # 1. load total number of registered electric vehicles by region
    evs_by_region = registered_electric_vehicles_by_regional_id(year=year)


    # 2. normalize the data
    ev_share_by_region = evs_by_region["number_of_registered_evs"] / evs_by_region["number_of_registered_evs"].sum()
    ev_share_by_region = ev_share_by_region.to_frame(name="ev_share")


    # 3. check if the sum of the ev_share_by_region is 1
    if not np.isclose(ev_share_by_region.sum(), 1.0):
        raise ValueError("The sum of the ev_share_by_region is not 1")


    return ev_share_by_region


def s2_future_ev_stock(year: int, szenario: str) -> pd.DataFrame:
    """
    Load the future ev stock for the given year and szenario.

    Args:
        year: int
            Year to load the data for
        szenario: str
            Szenario to load the data for

    Returns:
        float
    """

    # 1. load data
    data = load_future_ev_stock_s2()

    # 3. validate scenario
    scenarios = data.columns.tolist()
    if szenario not in scenarios:
        raise ValueError(f"Szenario must be one of {scenarios} but is {szenario!r}")
    
    # 4. validate year bounds
    start_year = data.index.min()
    end_year = data.index.max()
    if year < start_year or year > end_year:
        raise ValueError(f"Year must be between {start_year} and {end_year} but is {year}")
    
    # 5. reindex to every year & interpolate by index (i.e. by the year values)
    full_index = pd.RangeIndex(start_year, end_year + 1, name=data.index.name or "year")
    annual_df = (
        data
        .reindex(full_index)            # insert missing years
        .interpolate(method="index")    # linear interpolation weighted by year gaps
    )
    
    # 6. extract the single-year, single-scenario result
    annual_ev_stock = annual_df.at[year, szenario]


    return annual_ev_stock


def regional_dissaggregation_ev_consumption(ev_consumption: float) -> pd.DataFrame:
    """
    Dissaggregate the total number of electric vehicles in Germany to the regional level.

    Args:
        ev_consumption: pd.DataFrame
            with the column "power[mwh]"
        

    Returns:
        pd.DataFrame
            index: regional_id
            columns: power[mwh] per region
    """
    
    # 1. get the normalized regional distribution of EVs 
    ev_distribution_by_region = normalized_ev_distribution_by_region()


    # 2. get the number of evs by region for the given future year
    ev_consumption_by_region = ev_consumption * ev_distribution_by_region

    # 3. rename the column "ev_share" to "number_of_evs"
    ev_consumption_by_region = ev_consumption_by_region.rename(columns={"ev_share": "power[mwh]"})


    # 4. validation
    if not np.isclose(ev_consumption_by_region.sum(), ev_consumption):
        raise ValueError("The sum of the evs by region is not equal to the total number of evs in Germany")


    return ev_consumption_by_region
    

# S3
FIRST_YEAR_EXISTING_DATA_UGR = load_config()["first_year_existing_fuel_consumption_ugr"]
LAST_YEAR_EXISTING_DATA_UGR = load_config()["last_year_existing_fuel_consumption_ugr"]

def get_historical_vehicle_consumption_ugr_by_energy_carrier(year: int) -> pd.DataFrame:
    """
    Returns a DataFrame with the energy consumption of private households by energy carrier for a given year.

    Data is sourced from the UGR Table "85521-15: Energieverbrauch im Straßenverkehr, Energieträger in tiefer Gliederung, Deutschland, 2014 bis 2022"

    Args:
        year (int): The year for which to return the data.

    Returns:
        pd.DataFrame: 
            - index: year
            - columns: energy carriers [petrol[mwh], diesel[mwh], natural_gas[mwh], liquefied_petroleum_gas[mwh], bioethanol[mwh], biodiesel[mwh], biogas[mwh], power[mwh]] 
            - values: consumption in MWh
    """

    

    # 0. validate year - must be between 2014 and 2022
    if year < FIRST_YEAR_EXISTING_DATA_UGR or year > LAST_YEAR_EXISTING_DATA_UGR:
        raise ValueError(f"Year must be between {FIRST_YEAR_EXISTING_DATA_UGR} and {LAST_YEAR_EXISTING_DATA_UGR} but is {year}")

    # 1. Load the raw data
    df = load_historical_vehicle_consumption_ugr_by_energy_carrier()


    # 2. Filter for private households and allowed energy carriers
    df = df[df["Merkmal_1"] == "Private Haushalte"]


    # 3. Convert consumption from TJ to MWh
    # Replace comma with dot for decimal conversion, then convert to float
    df["Wert"] = df["Wert"].astype(float) * 1000 / 3.6


    # 4. Map German energy carrier names to English
    carrier_map = {
        "Benzin": "petrol[mwh]",
        "Bioethanol": "bioethanol[mwh]",
        "Diesel": "diesel[mwh]",
        "Biodiesel": "biodiesel[mwh]",
        "Erdgas": "natural_gas[mwh]",
        "Flüssiggas (Autogas)": "liquefied_petroleum_gas[mwh]",
        "Biogas (Biomethan)": "biogas[mwh]",
        "Strom": "power[mwh]"
    }
    df = df[df["Merkmal_2"].isin(carrier_map.keys())]
    df["energy_carrier"] = df["Merkmal_2"].map(carrier_map)


    # 5. Pivot the table (all years in index)
    result = df.pivot_table(
        index="Jahr",
        columns="energy_carrier",
        values="Wert",
        aggfunc="first"
    )
    # Ensure the index is named 'year' and is of type int
    result.index.name = "year"
    result.index = result.index.astype(int)

    # 6. Filter for the requested year after pivoting
    if year not in result.index:
        raise ValueError(f"Year {year} not found in the data.")
    result = result.loc[[year]]


    return result



def get_future_vehicle_consumption_ugr_by_energy_carrier(year: int, end_year: int = 2045, force_preprocessing: bool = False) -> pd.DataFrame:
    """
    Returns a DataFrame with the energy consumption of private households by energy carrier for a given year.

    The assumptions:
        1. the consumption for all energy_carriers  will be zero by 2045  exept for power[mwh]
        2. the consumption data of all erergy carriers will be transfered to power consumption
        3. the transition will happen in a linear way
        4. the total consumption will be the same for every year 


    Warning:
        - get_historical_vehicle_consumption_ugr_by_energy_carrier() 
            - must return a dataframe with the column "power[mwh]"
            - must contain only one row


    Args:
        year: int
            The year for which to return the data.
        force_preprocessing: bool
            If True, the function is not getting the data from the cache but is recalculating it

    Returns:
        pd.DataFrame: 
            - index: year
            - columns: energy carriers [petrol[mwh], diesel[mwh], natural_gas[mwh], liquefied_petroleum_gas[mwh], bioethanol[mwh], biodiesel[mwh], biogas[mwh], power[mwh]] 
            - values: consumption in MWh
    """

    # 0. validate input: must be between last year of existing data and 2045
    if year < LAST_YEAR_EXISTING_DATA_UGR or year > 2045:
        raise ValueError(f"Year must be between {LAST_YEAR_EXISTING_DATA_UGR} and 2045 but is {year}")
    
    # 0.1 Load config and get results from cache if available
    config = load_config("base_config.yaml")
    processed_dir = config["s3_future_ev_consumption_cache_dir"]
    processed_file = config["s3_future_ev_consumption_cache_file"]
    preprocessed_file_path = f"{processed_dir}/{processed_file}"
    if not force_preprocessing and os.path.exists(preprocessed_file_path):
        final_df = pd.read_csv(preprocessed_file_path, index_col=0)
        final_df = final_df.loc[[year]]
        return final_df


    # 1. load last year of existing data & validate it
    historic_consumption_df = get_historical_vehicle_consumption_ugr_by_energy_carrier(LAST_YEAR_EXISTING_DATA_UGR)
    if historic_consumption_df.shape[0] != 1:
        raise ValueError("`historic_consumption_df` must contain exactly one row")
    if "power[mwh]" not in historic_consumption_df.columns:
        raise ValueError("power[mwh] not found in historic_consumption_df columns")


    # 2. extract basics
    row = historic_consumption_df.iloc[0]
    other_cols = [c for c in historic_consumption_df.columns if c != "power[mwh]"]
    # calculate the total consumption u.a. for plausability check
    base_total = row.sum()


    # 3. skeleton with historic_consumption_df & end rows
    skeleton = pd.DataFrame(index=[LAST_YEAR_EXISTING_DATA_UGR, end_year],
                            columns=historic_consumption_df.columns,
                            dtype=float)
    skeleton.loc[LAST_YEAR_EXISTING_DATA_UGR] = row
    skeleton.loc[end_year, other_cols] = 0.0
    skeleton.loc[end_year, "power[mwh]"] = np.nan


    # 4. reindex full range and interpolate linearly
    full = skeleton.reindex(range(LAST_YEAR_EXISTING_DATA_UGR, end_year + 1))
    full = full.interpolate(method="linear", axis=0)


    # 5. compute power so each year's total equals base_total
    full["power[mwh]"] = base_total - full[other_cols].sum(axis=1)
    final_df =  full.round(3)


    # 6. pausability check
    final_total = full.sum(axis=1).values
    totals_ok = np.isclose(final_total, base_total)
    if not totals_ok.all():
        bad_years = full.index[~totals_ok].tolist()
        raise ValueError(f"Plausibility check failed: total consumption mismatch in years {bad_years}.")
    

    # 7. save to cache
    os.makedirs(processed_dir, exist_ok=True)
    final_df.to_csv(preprocessed_file_path)


    # 8. filter for the requested year
    final_df = final_df.loc[[year]]


    return final_df



