import pandas as pd
from netCDF4 import Dataset, num2date


from src.configs.mappings import *
from src.utils.utils import *
from src.data_access.local_reader import *
from src.data_processing.temperature import *


def cop_ts(sink_t: float = 40, source: str = 'ambient', delta_t: float = None, cf: float = 0.85, year: int = 2019):
    """
    Creates COP timeseries for ground/air/water heat pumps with floor heating.

    
    Args:
        sink_t : float, default 40
            temperature level of heat sink
        source : str, must be in ['ambient', 'waste heat'], default 'ambient'
            defines heat source for heat pump. If 'ambient', sink_t must be
            defined. if 'waste heat' delta_t must be defined.
        delta_t : float, default None.
            must be defined, if source is set to 'waste heat'. defines temperature
            difference between sink and source for heat pump. should not exceed 80.
        cf : float, default 0.85
            correction factor to be multiplied with COP-series to account for
            real world real-world effects, as opposed to ideal conditions under
            which the initial regression data was obtained.
        year : int, default 2019
    
    Returns:
        air_floor_cop : pd.DataFrame
            index = datetimeindex
            columns = Districts
        ground_floor_cop : pd.DataFrame
            index = datetimeindex
            columns = Districts
        water_floor_cop : pd.DataFrame
            index = datetimeindex
            columns = Districts
    """

    # 0. validate input
    if source not in ['ambient', 'waste_heat']:
       raise ValueError("'source' needs to be in ['ambient', 'waste_heat'].")
    if source == "waste_heat":
        assert isinstance(delta_t, (int, float)), ("'delta_t' needs to be a number.")
    

    # 1. get weather year
    #TODO: check if this is fine so:
    """ orginal Code:
    base_year = year
    if year > 2018:
        # ERA temperature data is only available for 2008-2018
        year = hist_weather_year().get(year)
    aber in hist year wird auf jahre gemapped die nicht in ERA temperature daten enthalten sind. e.g. 2019 wird auf 2007 gemapped. -> bug
    """
    #TODO: meine Lösung:
    base_year = year
    if year > 2018:
        # ERA temperature data is only available for 2008-2018
        year = 2018
    elif year < 2008:
        year = 2008
    else:
        year = year



    # 2. create empty df
    hours_per_year = get_hours_of_year(year)
    date = (pd.date_range((str(base_year) + '-01-01'), periods=hours_per_year, freq='h'))


    # 3. get source temperatures
    soil_t = soil_temp(year)
    soil_t.index = date
    # heat loss between soil and brine
    ground_t = soil_t.sub(5)


    # 4. normalize the regional_ids of ground_t
    ground_t = normalize_region_ids_average(ground_t)


    # 5. get air temperature
    air_t = get_temp_outside_hourly_for_regions(year=year)
    air_t.index = date




    # create water temperatur DataFrame (constant temperature 0f 10°C,
    # heat loss between water and brine)
    water_t = pd.DataFrame(index=ground_t.index,
                           columns=ground_t.columns,
                           data=10 - 5)
    if source == 'ambient':
        # sink temperature
        sink_t = pd.DataFrame(index=ground_t.index,
                              columns=ground_t.columns,
                              data=sink_t)
        # create cop timeseries based on temperature difference between source
        # and sink
        air_floor_cop = cop_curve((sink_t - air_t), 'air')
        ground_floor_cop = cop_curve((sink_t - ground_t), 'ground')
        water_floor_cop = cop_curve((sink_t - water_t), 'water')

        # change columns to int
        air_floor_cop.columns = air_floor_cop.columns.astype(int)
        ground_floor_cop.columns = ground_floor_cop.columns.astype(int)
        water_floor_cop.columns = water_floor_cop.columns.astype(int)

        dfs =  [air_floor_cop * cf, ground_floor_cop * cf, water_floor_cop * cf]
        

    # if source == 'waste heat' 
    else:
        
        # temperature difference between source and sink
        delta_t = pd.DataFrame(index=ground_t.index, columns=ground_t.columns, data=delta_t)
        # create cop timeseries based on temperature difference between source
        # and sink. use regression function from Arpagaus et al. (2018)
        # "Review - High temperature heat pumps: Market overview, state of
        #  the art, research Status, refrigerants, and application potentials",
        # Energy, 2018
        high_temp_hp_cop = 68.455 * (delta_t.pow(-0.76))

        high_temp_hp_cop.columns = high_temp_hp_cop.columns.astype(int)

        dfs = [high_temp_hp_cop * cf]

    return dfs










################################################################################################################################
#  Following functions for heat demand and COP are completly or partly taken
# from https://github.com/oruhnau/when2heat (Ruhnau et al.)
################################################################################################################################
def soil_temp(year: int):
    """
    Reads and processes soil temperature timeseries

    Args:
        year : int

    Returns:
        pd.DataFrame
            index = datetimeindex
            columns = Districts
    """

    # 0. validate input
    if 2008 > year > 2018:
        raise ValueError(f"No ERA_temperature data available for year {year}. see {load_config('base_config.yaml')['era_temperature_data_cache_dir']}")

    # -----------------------------------------------
    # from read.weather (When2Heat)

    # read weather nc
    nc = load_ERA_temperature_data(year)

    time = nc.variables['time'][:]
    time_units = nc.variables['time'].units
    latitude = nc.variables['latitude'][:]
    longitude = nc.variables['longitude'][:]
    variable = nc.variables['stl4'][:]

    # Transform to pd.DataFrame
    df = pd.DataFrame(data=variable.reshape(len(time), len(latitude) * len(longitude)),
                      index=pd.Index(num2date(time, time_units), name='time'),
                      columns=pd.MultiIndex.from_product([latitude, longitude], names=('latitude', 'longitude')))
    df.index = pd.to_datetime(df.index.astype(str))
    # ------------------------------------------------
    # upsample from 6h resolution
    df = upsample_df(df, '60min')

    # ------------------------------------------------
    # get LK and representative coords
    DF = load_shapefiles_by_regional_id()
    # Derive lat/lon tuple as representative point for each shape
    DF['coords'] = DF.geometry.apply(
        lambda x: x.representative_point().coords[:][0])
    DF['coords_WGS84'] = DF.to_crs({'init': 'epsg:4326'}).geometry.apply(
        lambda x: x.representative_point().coords[:][0])

    # ------------------------------------------------

    DF['ags_lk'] = DF.id_ags.floordiv(1000)

    # round to Era 0.75 x 0.75° grid
    def round_grid(x):
        return round(x * (4 / 3)) / (4 / 3)

    # create dataframe with soil temperature timeseries per LK
    soil_t = pd.DataFrame(index=df.index)
    soil_t.index.name = None

    # find lk representative coords in era5 soil temp
    soil_t_dict = {}
    # Collect all new columns in a dictionary instead of modifying soil_t in each iteration
    for coords in DF['coords_WGS84']:
        lat = round_grid(coords[1])
        lon = round_grid(coords[0])
        ags_lk = DF.loc[DF['coords_WGS84'] == coords, 'ags_lk'].values[0]
        soil_t_dict[ags_lk] = df.loc[:, lat].loc[:, lon]  # Store column in dict
    # Convert dictionary to a DataFrame and concatenate in one batch operation
    soil_t = pd.concat([soil_t, pd.DataFrame(soil_t_dict)], axis=1)

    # from Kelvin to Celsius
    soil_t = soil_t.sub(275.15)

    # sort columns
    soil_t = soil_t.reindex(sorted(soil_t.columns), axis=1)

    return soil_t


def cop_curve(delta_t, source_type):
    """
    Creates cop timeseries based on temperature difference between
    source and sink

    Args:
        delta_t : pd.DataFrame
            index = datetimeindex
            columns = District
        source_type : str
            must be in ['ground', 'air', 'water']

    Returns:
        pd.DataFrame
            index = datetimeindex
            columns = Districts
    """

    # 0. validate input
    if source_type not in ['ground', 'air', 'water']:
        raise ValueError("'source_type' needs to be in ['ground', 'air', 'water'].")

    # 1. load cop parameters
    cop_params = load_cop_parameters()

    # 2. clip delta_t
    delta_t.clip(lower=15, inplace=True)
    
    
    return sum(cop_params.loc[i, source_type] * delta_t ** i for i in range(3))


def upsample_df(df, resolution):  # from misc.upsample_df (When2Heat)
    """
    Resamples DataFrame to given resolution

    Parameters
    ----------
    df : pd.DataFrame
    resolution : str

    Returns
    -------
    pd.DataFrame
    """
    # The low-resolution values are applied to all high-resolution values up
    # to the next low-resolution value
    # In particular, the last low-resolution value is extended up to
    # where the next low-resolution value would be

    df = df.copy()

    # Determine the original frequency
    freq = df.index[-1] - df.index[-2]

    # Temporally append the DataFrame by one low-resolution value
    df.loc[df.index[-1] + freq, :] = df.iloc[-1, :]

    # Up-sample
    df = df.resample(resolution).ffill()

    # Drop the temporal low-resolution value
    df.drop(df.index[-1], inplace=True)

    return df



