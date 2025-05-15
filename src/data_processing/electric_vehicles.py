import pandas as pd
from src.data_access.local_reader import *
from src.utils.utils import *
import datetime





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
        ev_consumption.rename(columns={'number_of_registered_evs': 'power[mwh]'}, inplace=True)

        # 4. make the ev_consumption[mwh] column float
        ev_consumption['power[mwh]'] = ev_consumption['power[mwh]'].astype(float)

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


def get_normalized_ev_distribution_by_region() -> pd.DataFrame:
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
    ev_distribution_by_region = get_normalized_ev_distribution_by_region()


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
        4. the total driven distance will be the same for every year (efficency factor)


    Warning:
        - get_historical_vehicle_consumption_ugr_by_energy_carrier() 
            - must return a dataframe with the column "power[mwh]"
            - must contain only one row


    Args:
        year: int
            The year for which to return the data.
        force_preprocessing: bool
            If True, the function is not getting the data from the cache but is recalculating it
        end_year: int
            The year to which the data is projected zero for all energy carriers except power[mwh]

    Returns:
        pd.DataFrame: 
            - index: requested year
            - columns: energy carriers [petrol[mwh], diesel[mwh], natural_gas[mwh], liquefied_petroleum_gas[mwh], bioethanol[mwh], biodiesel[mwh], biogas[mwh], power[mwh]] 
            - values: consumption in MWh
    """

    # 0. validate input: must be between last year of existing data and 2045
    if year < LAST_YEAR_EXISTING_DATA_UGR or year > 2045:
        raise ValueError(f"Year must be between {LAST_YEAR_EXISTING_DATA_UGR} and 2045 but is {year}")
    

    # 0.1 Load config and get results from cache if available
    base_year = LAST_YEAR_EXISTING_DATA_UGR

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
    if historic_consumption_df.index.name != "year":
        raise ValueError("year not found in historic_consumption_df index")


    # 2. Identify fuels and set efficiencies
    fuel_cols = [col for col in historic_consumption_df.columns if col not in ('year', 'power[mwh]')]
    efficiency = {
        'biodiesel[mwh]': 0.37,
        'bioethanol[mwh]': 0.38,
        'biogas[mwh]': 0.39,
        'diesel[mwh]': 0.37,
        'liquefied_petroleum_gas[mwh]': 0.36,
        'natural_gas[mwh]': 0.39,
        'petrol[mwh]': 0.38,
    }

    # Capture the original base_year values
    orig = historic_consumption_df.loc[base_year]


    # 3. Prepare the projection DataFrame
    years = np.arange(base_year, end_year + 1)
    proj = pd.DataFrame(index=years)


    # 4. Linearly interpolate each fuel to zero
    span = end_year - base_year
    for f in fuel_cols:
        proj[f] = orig[f] * (1 - (proj.index - base_year) / span)
        proj[f] = proj[f].clip(lower=0)


    # 5. Compute saved energy per fuel
    saved = pd.DataFrame(index=years, columns=fuel_cols)
    for f in fuel_cols:
        saved[f] = orig[f] - proj[f]


    # 6. Compute additional power from saved energy
    delta_power = pd.Series(0, index=years)
    for f in fuel_cols:
        eff = efficiency.get(f, 0)
        delta_power += saved[f] * eff


    # 7. Build projected power series
    proj['power[mwh]'] = orig['power[mwh]'] + delta_power


    # 8. set the index to year
    proj.index.name = "year"


    # 9. pausability check
    if proj.isnull().any().any():
        raise ValueError("There are nan values in the projected power series!")


    # 10. save to cache
    os.makedirs(processed_dir, exist_ok=True)
    proj.to_csv(preprocessed_file_path)


    # 11. filter for the requested year
    proj = proj.loc[[year]]


    return proj


def get_fiture_s3():


    # 1. load last year of existing data & validate it
    historic_consumption_df = get_historical_vehicle_consumption_ugr_by_energy_carrier(LAST_YEAR_EXISTING_DATA_UGR)
    if historic_consumption_df.shape[0] != 1:
        raise ValueError("`historic_consumption_df` must contain exactly one row")
    if "power[mwh]" not in historic_consumption_df.columns:
        raise ValueError("power[mwh] not found in historic_consumption_df columns")
    if historic_consumption_df.index.name != "year":
        raise ValueError("year not found in historic_consumption_df index")



    df0 = historic_consumption_df
    base_year = int(df0.index[0])
    target_year = 2045

    # 2. Identify fuels and set efficiencies
    fuel_cols = [col for col in df0.columns if col not in ('year', 'power[mwh]')]
    # TODO: fill in real efficiency factors here (between 0 and 1)
    efficiency = {
        'biodiesel[mwh]': 0.37,
        'bioethanol[mwh]': 0.38,
        'biogas[mwh]': 0.39,
        'diesel[mwh]': 0.37,
        'liquefied_petroleum_gas[mwh]': 0.36,
        'natural_gas[mwh]': 0.39,
        'petrol[mwh]': 0.38,
    }

    # Capture the original 2022 values
    orig = df0.loc[base_year]

    # 3. Prepare the projection DataFrame
    years = np.arange(base_year, target_year + 1)
    proj = pd.DataFrame(index=years)

    # 4. Linearly interpolate each fuel to zero
    span = target_year - base_year
    for f in fuel_cols:
        proj[f] = orig[f] * (1 - (proj.index - base_year) / span)
        proj[f] = proj[f].clip(lower=0)

    # 5. Compute saved energy per fuel
    saved = pd.DataFrame(index=years, columns=fuel_cols)
    for f in fuel_cols:
        saved[f] = orig[f] - proj[f]

    # 6. Compute additional power from saved energy
    delta_power = pd.Series(0, index=years)
    for f in fuel_cols:
        eff = efficiency.get(f, 0)
        delta_power += saved[f] * eff

    # 7. Build projected power series
    proj['power[mwh]'] = orig['power[mwh]'] + delta_power

    # 8. (Optional) reset index if you like
    proj = proj.reset_index().rename(columns={'index': 'year'})


    return proj



# ev charging profile

def get_normalized_daily_ev_charging_profile(type: str, day_type: str) -> pd.DataFrame:
    """
    Load the normalized ev charging profile for the given type and day type.

    Args:
        type: str
            The type of the ev charging profile ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'total']
        day_type: str
            The day type of the ev charging profile ['workday', 'weekend']

    """

    # 0. validate input
    if type not in ["h1", "h2", "h3", "h4", "h5", "h6", "total"]:
        raise ValueError(f"Type must be one of ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'total'] but is {type}")
    if day_type not in ["workday", "weekend"]:
        raise ValueError(f"Day type must be one of ['workday', 'weekend'] but is {day_type}")

    # 1. load data
    ev_charging_profile = load_ev_charging_profile(type=type, day_type=day_type)

    # 2. cut the last entry of the dataframe since the data is one 10min step too long (145 instead of 144)
    ev_charging_profile = ev_charging_profile.iloc[:-1]

    # 2. normalize the data
    ev_charging_profile_normalized = ev_charging_profile / ev_charging_profile.values.sum().sum()

    # 3. rename the columns of the dataframe: 
    ev_charging_profile_normalized.rename(columns={
        "home_charging[kw/car]":"home_charging",
        "work_charging[kw/car]":"work_charging",
        "public_charging[kw/car]":"public_charging"
    }, inplace=True)




    # 3. validate the result
    if not np.isclose(ev_charging_profile_normalized.sum().sum(), 1.0):
        raise ValueError("The sum of the ev charging profile is not 1!")
    if ev_charging_profile_normalized.isnull().any().any():
        raise ValueError("There are nan values in the ev charging profile!")


    return ev_charging_profile_normalized



def disaggregate_temporal_ev_consumption_for_state(ev_consumption_by_regional_id: pd.DataFrame, state: str, year: int, yearly_charging_profile: pd.DataFrame) -> pd.DataFrame:
    """
    Disaggregate the ev consumption by charging profile over the year in 10min steps.

    Args:
        ev_consumption_by_regional_id: pd.DataFrame
            The ev consumption by regional id
        state: str
            The state of the ev consumption
        year: int
            The year of the ev consumption
    """

    # 0. validate input
    if state not in federal_state_dict().values():
        raise ValueError(f"state must be in {federal_state_dict().values()}")


    # 1. filter regional_ids for the given state
    ev_consumption = ev_consumption_by_regional_id.loc[
        [federal_state_dict().get(int(str(x)[:-3])) == state for x in ev_consumption_by_regional_id.index]
    ]
    



    
    

    # 5. iterate over every regional_id and disaggregate the ev consumption by yearly_charging_profile
    # 5.1. create a list of the disaggregated profiles
    regional_profiles_list = []
    regional_ids = []

    # 5.2. iterate over every regional_id 
    for regional_id, row in ev_consumption.iterrows():
        total_regional_consumption_mwh = row['power[mwh]']
        
        # Multiply the normalized profile by the total consumption for this region
        # This scales the distribution to the region's total annual consumption
        disaggregated_regional_consumption = yearly_charging_profile * total_regional_consumption_mwh
        
        regional_profiles_list.append(disaggregated_regional_consumption)
        regional_ids.append(regional_id)

    # Concatenate all regional profiles into a single DataFrame
    # 6. The 'keys' argument creates the top level of the MultiIndex for columns
    if not regional_profiles_list:
        # Return an empty DataFrame with appropriate structure if ev_consumption was empty
        ev_consumption_by_regional_id_temporal = pd.DataFrame(index=yearly_charging_profile.index)
        ev_consumption_by_regional_id_temporal.columns = pd.MultiIndex.from_tuples([], names=['regional_id', 'charging_location'])
        return ev_consumption_by_regional_id_temporal


    # 7. concatenate the disaggregated profiles
    ev_consumption_by_regional_id_temporal = pd.concat(regional_profiles_list, axis=1, keys=regional_ids)
    

    # 8. Name the levels of the column MultiIndex for clarity
    ev_consumption_by_regional_id_temporal.columns.names = ['regional_id', 'charging_location']
    

    # 9. validate the result
    if not np.isclose(ev_consumption_by_regional_id_temporal.sum().sum(), ev_consumption.sum().sum()):
        raise ValueError("The sum of the ev consumption by regional id temporal is not equal to the sum of the ev consumption by regional id!")


    return ev_consumption_by_regional_id_temporal






def get_normalized_yearly_ev_charging_profile(year: int, state: str) -> pd.DataFrame:
    """
    Generate the yearly charging profile for the given state and year.

    TODO: description & cleanup
    """


    #2. build the mask
    mask = create_weekday_workday_holiday_mask(state=state, year=year)


    # 3. load the charging profiles
    ev_charging_profile_workday = get_normalized_daily_ev_charging_profile(type="total", day_type="workday")
    ev_charging_profile_weekend = get_normalized_daily_ev_charging_profile(type="total", day_type="weekend")


    # 0. Multiply the charging profiles by 1,000,000
    #    (as per user request, values won't be "that small" temporarily)
    profile_workday_scaled = ev_charging_profile_workday 
    profile_weekend_scaled = ev_charging_profile_weekend

    # Convert string time index of daily profiles to datetime.time objects for easier lookup
    profile_workday_scaled.index = pd.to_datetime(profile_workday_scaled.index, format='%H:%M:%S').time
    profile_weekend_scaled.index = pd.to_datetime(profile_weekend_scaled.index, format='%H:%M:%S').time

    # 1. Build a DataFrame of a year in 10-minute steps
    start_datetime = datetime.datetime(year, 1, 1, 0, 0, 0)
    end_datetime = datetime.datetime(year, 12, 31, 23, 50, 0)
    yearly_index = pd.date_range(start=start_datetime, end=end_datetime, freq='10min')
    
    yearly_load_profile = pd.DataFrame(index=yearly_index, columns=profile_workday_scaled.columns)
    yearly_load_profile.index.name = 'datetime'

    # 2. Fill this DataFrame with the correct values: for each day in mask, select the appropriate scaled daily profile
    print("Filling yearly load profile...")
    for day_date_ts, day_mask_info in mask.iterrows():
        # day_date_ts is a Timestamp object from the mask's index
        day_date_obj = day_date_ts.date() # Convert to datetime.date for comparison if needed

        # Select the appropriate scaled daily profile
        if day_mask_info['workday']:
            active_daily_profile = profile_workday_scaled
        elif day_mask_info['weekend_holiday']: # Ensure this covers all non-workdays
            active_daily_profile = profile_weekend_scaled
        else:
            print(f"Warning: Day {day_date_obj} is neither workday nor weekend_holiday in mask. Using weekend profile as fallback.")
            active_daily_profile = profile_weekend_scaled


        # Get all timestamps in the yearly_load_profile for the current day_date_obj
        # Efficiently slice the part of yearly_load_profile for the current day
        current_day_timestamps_in_yearly_profile = yearly_load_profile.loc[yearly_load_profile.index.date == day_date_obj].index
        
        if not current_day_timestamps_in_yearly_profile.empty:
            # Assign values from the selected daily profile
            # The .values assignment works if rows are aligned (144 per day)
            try:
                yearly_load_profile.loc[current_day_timestamps_in_yearly_profile] = active_daily_profile.values
            except Exception as e:
                # Fallback to row-by-row if shapes mismatch or other issues (slower)
                print(f"Direct assignment failed for {day_date_obj}, falling back. Error: {e}")
                for ts in current_day_timestamps_in_yearly_profile:
                    time_obj = ts.time() # datetime.time object
                    yearly_load_profile.loc[ts] = active_daily_profile.loc[time_obj]
        else:
            print(f"Warning: No timestamps found in yearly_profile for {day_date_obj}. Mask date might be out of range or year mismatch.")


    # 3. Normalize all values so the sum of all values equals 1,000,000
    print("Normalizing final yearly profile...")
    # Sum of all values in the dataframe (sum over all columns, then sum these totals)
    current_total_sum = yearly_load_profile.sum().sum()
    
    if current_total_sum == 0:
        print("Warning: Total sum of the yearly profile is 0. Cannot normalize.")
        # Handle this case, e.g., by returning the zero DataFrame or raising an error
    else:
        yearly_load_profile = (yearly_load_profile / current_total_sum)

    
    # 4. set the index to the datetime index
    yearly_load_profile.index = pd.to_datetime(yearly_load_profile.index)
        
    # 5. verification
    final_sum = yearly_load_profile.sum().sum()
    if not np.isclose(final_sum, 1.0):
        raise ValueError(f"Final sum after normalization is not 1.0 but {final_sum}")

    return yearly_load_profile
