import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import geopandas as gpd
from matplotlib.patches import Rectangle
from mpl_toolkits.axes_grid1 import make_axes_locatable




import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from src.data_processing.employees import *
from src.data_processing.consumption import *
from src.data_access.api_reader import *
from src.pipeline.pipe_consumption import *
from src.pipeline.pipe_applications import *
from src.data_access.local_reader import *
from src.data_processing.application import *
from src.data_processing.temporal import *
from src.data_processing.temperature import *
from src.pipeline.pipe_temporal import *
from src.pipeline.pipe_heat import *
from src.data_processing.heat import *
from src.data_processing.cop import *
from src.pipeline.pipe_ev_regional_consumption import *
from src.pipeline.pipe_ev_temporal import *

now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# utils to save plots with datetime
def save_plot_with_datetime(plt_obj, path, name, dpi=300):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{now}.jpg"
    path = os.path.join(path, filename)
    plt_obj.savefig(path, dpi=dpi)
    plt_obj.show()

def save_dataframe_with_datetime(df, name, path_output):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{now}.csv"
    path = os.path.join(path_output, filename)
    df.to_csv(path)
    print(f"Saved {filename} to {path}")




################### total consumption per year 2017-2045 ######################################################
path_consumption = "src/utils/thesis_outputs/ev_consumption/"
"""
"""
def data_total_ev_consumption_s2():

    szenarios = ["ambit", "trend", "regio"]
    for szenario in szenarios:
        df_total = pd.DataFrame()
        for year in range(2017, 2046):
            df = electric_vehicle_consumption_by_regional_id(year=year, szenario="KBA_2", s2_szenario=szenario,force_preprocessing=True)
            total_consumption = df.sum().sum()
            df_total = pd.concat([df_total, pd.DataFrame({"year": [year], "total_consumption": [total_consumption]})])
        save_dataframe_with_datetime(df_total, f"ev_consumption_by_regional_id_kba_2_{szenario}_{now}", path_consumption)


def data_total_ev_consumption():
    df_total = pd.DataFrame()

    for year in range(2017, 2046):
        df = electric_vehicle_consumption_by_regional_id(year=year, szenario="KBA_1",force_preprocessing=True)
        total_consumption = df.sum().sum()
        df_total = pd.concat([df_total, pd.DataFrame({"year": [year], "total_consumption": [total_consumption]})])

    save_dataframe_with_datetime(df_total, f"ev_consumption_by_regional_id_kba_1_{now}", path_consumption)

def graph_ev_consumption():
    files = {
        "KBA 1": "src/utils/thesis_outputs/ev_consumption/ev_consumption_by_regional_id_kba_1_20250521_225507_20250521_225509.csv",
        "KBA 2 - Trend": "src/utils/thesis_outputs/ev_consumption/ev_consumption_by_regional_id_kba_2_trend_20250521_224632_20250521_224910.csv",
        "KBA 2 - Regio": "src/utils/thesis_outputs/ev_consumption/ev_consumption_by_regional_id_kba_2_regio_20250521_224632_20250521_225044.csv",
        "KBA 2 - Ambit": "src/utils/thesis_outputs/ev_consumption/ev_consumption_by_regional_id_kba_2_ambit_20250521_224632_20250521_224835.csv",
        "UGR": "src/utils/thesis_outputs/ev_consumption/ev_consumption_by_regional_id_ugr_20250521_220240_20250521_220242.csv"
    }

    # Plot setup
    fig, ax = plt.subplots(figsize=(12, 6))

    # Load and plot each scenario
    for label, path in files.items():
        df = pd.read_csv(path)
        df["total_consumption_million"] = df["total_consumption"] / 1e6
        ax.plot(df["year"], df["total_consumption_million"], label=label)

    # Formatting
    ax.set_xlabel("Year")
    ax.set_ylabel("Total Electric Vehicle Consumption [TWh]")

    # Legend outside
    ax.legend(loc='lower left', bbox_to_anchor=(1, 0))
    ax.grid(True)

    plt.tight_layout()

    save_plot_with_datetime(plt, path_consumption, "ev_consumption_total", dpi=300)

#data_total_ev_consumption_s2()
#data_total_ev_consumption()
#graph_ev_consumption()



################### total consumption s3 devided by energy carriers ######################################################
path_consumption = "src/utils/thesis_outputs/ev_consumption_s3/"
def graph_ev_consumption_by_energy_carrier_s3():
    # Load data
    csv_path = "data/processed/electric_vehicles/s3_future_ev_consumption/s3_future_ev_consumption.csv"
    df = pd.read_csv(csv_path)

        
    # Set year as index for plotting
    df.set_index("year", inplace=True)

    # Define fuel types (all columns except year)
    fuel_columns = df.columns

    # Plotting
    fig, ax = plt.subplots(figsize=(10, 6))

    # Vertical stacked bar chart
    bottom = None
    for fuel in fuel_columns:
        ax.bar(df.index, df[fuel], bottom=bottom, label=fuel)
        if bottom is None:
            bottom = df[fuel]
        else:
            bottom += df[fuel]

    # Axis labeling
    ax.set_ylabel("Energy Consumption [MWh]")
    ax.set_xlabel("Year")
    ax.set_title("Energy Consumption by Fuel Type per Year")
    ax.legend(title="Fuel Type", bbox_to_anchor=(1.05, 1), loc="upper left")
    ax.grid(True, axis='y', linestyle='--', alpha=0.5)

    plt.tight_layout()
    plt.show()

#graph_ev_consumption_by_energy_carrier_s3()



################### total kba ev stock 2017-2045 ######################################################
path_consumption = "src/utils/thesis_outputs/ev_stock_kba/"
def data_total_ev_stock_historical(owner="commercial"):

    df_total = pd.DataFrame()
    
    for year in range(2017, 2025):

        number_of_registered_evs = registered_electric_vehicles_by_regional_id(year=year)["number_of_registered_evs"].sum().sum()
        if owner == "private":
            share_of_commercial_vehicles = share_of_commercial_vehicles_by_regional_id(year=year)["share_of_commercial_vehicles"].mean()
            number_of_commercial_vehicles = number_of_registered_evs * share_of_commercial_vehicles
            number_of_private_vehicles = number_of_registered_evs - number_of_commercial_vehicles
            total_stock = number_of_private_vehicles
        elif owner == "commercial":
            share_of_commercial_vehicles = share_of_commercial_vehicles_by_regional_id(year=year)["share_of_commercial_vehicles"].mean()
            number_of_commercial_vehicles = number_of_registered_evs * share_of_commercial_vehicles
            total_stock = number_of_commercial_vehicles

        df_total = pd.concat([df_total, pd.DataFrame({"year": [year], "total_stock": [total_stock]})])

    df_total.to_csv(os.path.join(path_consumption, f"ev_stock_historical_{owner}_kba_2017_2024.csv"), index=False)

def data_total_ev_stock_future_s1():
    df_total = pd.DataFrame()
    for year in range(2025, 2046):
        number_of_registered_evs = s1_future_ev_stock_15mio_by_2030(year=year)
        share_of_commercial_vehicles = share_of_commercial_vehicles_by_regional_id(year=year)["share_of_commercial_vehicles"].mean()

        number_of_commercial_vehicles = number_of_registered_evs * share_of_commercial_vehicles
        number_of_private_vehicles = number_of_registered_evs - number_of_commercial_vehicles
        
        total_stock = number_of_private_vehicles
        df_total = pd.concat([df_total, pd.DataFrame({"year": [year], "total_stock": [total_stock]})])

    df_total.to_csv(os.path.join(path_consumption, "ev_stock_total_kba_1_private_2025_2045.csv"), index=False)

def data_total_ev_stock_future_s2():
    df_total = pd.DataFrame()
    
    years = list(range(2025, 2046))
    szenarios = ["trend", "ambit", "regio"]
    data = {"year": years}
    for szenario in szenarios:
        data[szenario] = []

    for year in years:
        for szenario in szenarios:
            number_of_registered_evs = s2_future_ev_stock(year=year, szenario=szenario)
            share_of_commercial_vehicles = share_of_commercial_vehicles_by_regional_id(year=year)["share_of_commercial_vehicles"].mean()
            number_of_commercial_vehicles = number_of_registered_evs * share_of_commercial_vehicles
            number_of_private_vehicles = number_of_registered_evs - number_of_commercial_vehicles
            data[szenario].append(number_of_private_vehicles)

    df_total = pd.DataFrame(data)
    df_total.to_csv(os.path.join(path_consumption, "ev_stock_total_kba_2_private_2025_2045.csv"), index=False)


def graph_ev_stock_historical():
    # Load data
    df_hist = pd.read_csv("src/utils/thesis_outputs/ev_stock_kba/ev_stock_total_kba_2017_2024.csv")
    
    df_hist['total_stock'] /= 1e6

    # Create figure
    fig, ax = plt.subplots(figsize=(10, 5.625))


    ax.plot(df_hist['year'], df_hist['total_stock'], marker='o', markersize=4, label='Historisch')


    # x-axis: yearly ticks
    all_years = list(range(df_hist['year'].min(), df_hist['year'].max() + 1))
    ax.set_xticks(all_years)

    # Labels
    ax.set_title("EV Stock – Historical")
    ax.set_xlabel("Year")
    ax.set_ylabel("Mio EVs")

    # Grid, legend, layout
    ax.legend(loc='lower left', bbox_to_anchor=(1, 0))
    ax.grid(True)
    plt.tight_layout()

    save_plot_with_datetime(plt, "ev_stock_historical_2017_2024", dpi=300)
    
def graph_ev_stock_historical_stacked():
    df_com = pd.read_csv("src/utils/thesis_outputs/ev_stock_kba/ev_stock_historical_commercial_kba_2017_2024.csv")
    df_priv = pd.read_csv("src/utils/thesis_outputs/ev_stock_kba/ev_stock_historical_private_kba_2017_2024.csv")

    # Merge on year
    merged_df = pd.merge(df_com, df_priv, on="year")
    merged_df["total_stock"] = merged_df["total_stock_x"] + merged_df["total_stock_y"]

    # Convert to millions
    merged_df["total_stock"] /= 1e6
    merged_df["total_stock_y"] /= 1e6

    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot lines
    ax.plot(merged_df["year"], merged_df["total_stock"], label="Total EV Stock", marker="o")
    ax.plot(merged_df["year"], merged_df["total_stock_y"], label="Private EV Stock", marker="o")

    # Labels and layout
    ax.set_xlabel("Year")
    ax.set_ylabel("Number of electric vehicles [Mio]")
    ax.legend()
    plt.grid(True)
    plt.tight_layout()

    save_plot_with_datetime(plt, path_consumption, "ev_stock_historical_stacked", dpi=300)

def graph_ev_stock_complete():
    # Load data
    df_hist = pd.read_csv("src/utils/thesis_outputs/ev_stock_kba/ev_stock_total_private_kba_2017_2024.csv")
    df_kba1 = pd.read_csv("src/utils/thesis_outputs/ev_stock_kba/ev_stock_total_kba_1_private_2025_2045.csv")
    df_kba2 = pd.read_csv("src/utils/thesis_outputs/ev_stock_kba/ev_stock_total_kba_2_private_2025_2045.csv")
    
    df_hist['total_stock'] /= 1e6
    df_kba1['total_stock'] /= 1e6
    df_kba2['trend'] /= 1e6
    df_kba2['ambit'] /= 1e6
    df_kba2['regio'] /= 1e6

    # Create figure
    fig, ax = plt.subplots(figsize=(10, 5.625))

    ax.plot(df_hist['year'], df_hist['total_stock'], marker='o', markersize=4, label='Historical')
    ax.plot(df_kba1['year'], df_kba1['total_stock'], marker='o', markersize=4, label='KBA 1')
    ax.plot(df_kba2['year'], df_kba2['trend'], marker='o', markersize=4, label='KBA 2 Trend')
    ax.plot(df_kba2['year'], df_kba2['ambit'], marker='o', markersize=4, label='KBA 2 Ambit')
    ax.plot(df_kba2['year'], df_kba2['regio'], marker='o', markersize=4, label='KBA 2 Regio')

    # Highlight the year 2030
    ax.axvline(x=2030, color='red', linestyle='--', linewidth=1, label='2030')

    # x-axis: yearly ticks
    all_years = list(range(df_hist['year'].min(), df_kba2['year'].max() + 1, 2))
    ax.set_xticks(all_years)

    # Labels
    ax.set_xlabel("Year")
    ax.set_ylabel("Private Electric Vehicles in Germany [Mio]")

    # Add a horizontal line at 15 million
    ax.axhline(y=15, color='green', linestyle='--', linewidth=1, label='15 Million')

    # Grid, legend, layout
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1), ncol=3)
    ax.grid(True)
    plt.tight_layout()

    save_plot_with_datetime(plt, path_consumption, "ev_stock_complete_private", dpi=300)

def graph_ev_stock_historical_percentage():
    df_com = pd.read_csv("src/utils/thesis_outputs/ev_stock_kba/ev_stock_historical_commercial_kba_2017_2024.csv")
    df_priv = pd.read_csv("src/utils/thesis_outputs/ev_stock_kba/ev_stock_historical_private_kba_2017_2024.csv")

    # Merge on year
    merged_df = pd.merge(df_com, df_priv, on="year")
    merged_df["total_stock"] = merged_df["total_stock_x"] + merged_df["total_stock_y"]
    
    # Calculate percentage of private vehicles
    merged_df["private_percentage"] = (merged_df["total_stock_y"] / merged_df["total_stock"]) * 100

    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot line
    ax.plot(merged_df["year"], merged_df["private_percentage"], label="Share of Private EVs", marker="o", color="darkblue")

    # Labels and layout
    ax.set_xlabel("Year")
    ax.set_ylabel("Share of private electric vehicles [%]")
    ax.set_ylim(0, 100)  # Set y-axis from 0 to 100%
    ax.legend()
    plt.grid(True)
    plt.tight_layout()

    save_plot_with_datetime(plt, path_consumption, "ev_stock_historical_percentage", dpi=300)

#data_total_ev_stock_historical(owner="commercial")
#data_total_ev_stock_historical(owner="private")
#data_total_ev_stock_future_s1()
#data_total_ev_stock_future_s2()
#graph_ev_stock_complete()
#graph_ev_stock_historical()
#graph_ev_stock_historical_stacked()
#graph_ev_stock_historical_percentage()

################### kba ev stock 2024 by region ######################################################
path_consumption = "src/utils/thesis_outputs/ev_stock_kba/"
def graph_ev_stock_kba_2024():
    # Load EV data and normalize regional_id to string

    # Load the AGS to NUTS3 mapping file
    nuts3_map = pd.read_csv("src/utils/thesis_outputs/t_nuts3_lk.csv", dtype={"id_ags": str, "natcode_nuts3": str})
    nuts3_map["ags_5"] = nuts3_map["id_ags"].str[:-3]
    
    ev_df = registered_electric_vehicles_by_regional_id(year=2024)
    """
    ev_df
                number_of_registered_evs
    regional_id                          
    8111                            17944
    8115                            15953
    8116                            14856
    """
    ev_df["regional_id"] = ev_df.index.astype(str)
    ev_df["ags_5"] = ev_df["regional_id"].str[:5]


    ev_df = ev_df.merge(nuts3_map[["ags_5", "natcode_nuts3"]], on="ags_5", how="left")
    ev_df.drop(columns=["regional_id", "ags_5"], inplace=True)
    ev_df.rename(columns={"natcode_nuts3": "nuts3"}, inplace=True)

    """
    ev_df
        number_of_registered_evs  nuts3
    0                       17944  DE111
    1                       15953  DE112
    2                       14856  DE113
    """

    # Load the GeoJSON NUTS3 shapefile from Eurostat
    gdf = gpd.read_file("src/utils/thesis_outputs/NUTS_RG_20M_2024_4326.geojson")  # update path
    gdf = gdf[(gdf["LEVL_CODE"] == 3) & (gdf["CNTR_CODE"] == "DE")]

    merged_gdf = gdf.merge(ev_df, left_on="NUTS_ID", right_on="nuts3", how="left")
    merged_gdf["number_of_registered_evs"] = merged_gdf["number_of_registered_evs"].fillna(0)

    # Plotting
    fig, ax = plt.subplots(1, 1, figsize=(10, 12))
    plot = merged_gdf.plot(
        column="number_of_registered_evs",
        cmap="plasma",
        linewidth=0.6,
        edgecolor="black",
        ax=ax,
        legend=False
    )

    # Get colormap information for colorbar
    norm = plt.Normalize(vmin=merged_gdf["number_of_registered_evs"].min(),
                        vmax=merged_gdf["number_of_registered_evs"].max())
    sm = plt.cm.ScalarMappable(cmap="plasma", norm=norm)
    sm._A = []  # dummy array for colorbar

    # Add a colorbar right under the plot with reduced spacing
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("bottom", size="5%", pad=0.1)  # smaller pad
    cbar = fig.colorbar(sm, cax=cax, orientation="horizontal")
    cbar.set_label("Number of Registered EVs")

    # Draw rectangle around Germany
    bounds = merged_gdf.total_bounds  # [minx, miny, maxx, maxy]
    minx, miny, maxx, maxy = bounds
    x_margin = (maxx - minx) * 0.02
    y_margin = (maxy - miny) * 0.02
    rect = Rectangle((minx - x_margin, miny - y_margin),
                    maxx - minx + 2 * x_margin,
                    maxy - miny + 2 * y_margin,
                    linewidth=1.2, edgecolor='black', facecolor='none')
    ax.add_patch(rect)

    ax.axis("off")
    plt.tight_layout()

    save_plot_with_datetime(plt, "ev_stock_total_map_kba_2024", dpi=300)

#graph_ev_stock_kba_2024()



################### charging profiles normalized ######################################################
path_consumption = "src/utils/thesis_outputs/ev_charging_profiles/"
def data_charging_profiles_all():
    df1 = get_normalized_daily_ev_charging_profile_all(type="total", day_type="workday")
    df2 = get_normalized_daily_ev_charging_profile_all(type="total", day_type="weekend")

    df1.index = pd.to_datetime(df1.index, format='%H:%M:%S').time
    df2.index = pd.to_datetime(df2.index, format='%H:%M:%S').time
    
    df1.index.name = 'time'
    df2.index.name = 'time'

    df1.to_csv(os.path.join(path_consumption, "ev_charging_profile_normalized_total_workday_all.csv"))
    df2.to_csv(os.path.join(path_consumption, "ev_charging_profile_normalized_total_weekend_all.csv"))

def data_charging_profiles_home():
    df1 = get_normalized_daily_ev_charging_profile_home(type="total", day_type="workday")
    df2 = get_normalized_daily_ev_charging_profile_home(type="total", day_type="weekend")

    df1.index = pd.to_datetime(df1.index, format='%H:%M:%S').time
    df2.index = pd.to_datetime(df2.index, format='%H:%M:%S').time

    df1.index.name = 'time'
    df2.index.name = 'time' 

    df1.to_csv(os.path.join(path_consumption, "ev_charging_profile_normalized_total_workday_home.csv"))
    df2.to_csv(os.path.join(path_consumption, "ev_charging_profile_normalized_total_weekend_home.csv"))

def graph_charging_profiles_all():
    # ! muss für workday und weekend getrennt gemacht werden

    data_path = os.path.join(path_consumption, 'ev_charging_profile_normalized_total_weekend_all.csv')
    df = pd.read_csv(data_path)

    # Convert 'time' to datetime for better plotting
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S')

    # Plotting
    plt.figure(figsize=(12, 6))

    plt.plot(df['time'], df['home_charging'], label='Home Charging')
    plt.plot(df['time'], df['work_charging'], label='Work Charging')
    plt.plot(df['time'], df['public_charging'], label='Public Charging')

    # Formatting plot
    plt.title('Normalized EV Charging Profiles (Weekend)', fontsize=16)
    plt.xlabel('Time of Day', fontsize=14)
    plt.ylabel('Normalized Energy Consumption', fontsize=14)
    plt.grid(True)

    # Improve x-axis time formatting
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))
    plt.xticks(rotation=45)

    plt.legend()
    plt.tight_layout()

    save_plot_with_datetime(plt, "ev_charging_profile_normalized_total_weekend_all", dpi=300)

def graph_charging_profiles_home():
    # ! muss für workday und weekend getrennt gemacht werden

    data_path = os.path.join(path_consumption, 'ev_charging_profile_normalized_total_weekend_home.csv')
    df = pd.read_csv(data_path)

    # Convert 'time' to datetime for better plotting
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S')

    # Plotting
    plt.figure(figsize=(12, 6))

    plt.plot(df['time'], df['home_charging'], label='Home Charging')

    # Formatting plot
    plt.title('Normalized EV Charging Profiles (Weekend)', fontsize=16)
    plt.xlabel('Time of Day', fontsize=14)
    plt.ylabel('Normalized Energy Consumption', fontsize=14)
    plt.grid(True)

    # Improve x-axis time formatting
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))
    plt.xticks(rotation=45)

    plt.legend()
    plt.tight_layout()

    save_plot_with_datetime(plt, "ev_charging_profile_normalized_total_weekend_home", dpi=300)

#data_charging_profiles_home()
#data_charging_profiles_all()
#graph_charging_profiles_all()
#graph_charging_profiles_home()


################### UGR consumption histroical 2014-2022 ######################################################
path_output = "src/utils/thesis_outputs/ev_consumption_ugr_historical/"

def data_ugr_consumption_historical():
    df_total = pd.DataFrame()

    for year in range(2014, 2023):
        df = get_historical_vehicle_consumption_ugr_by_energy_carrier(year=year)
        df_total = pd.concat([df_total, df])

    df_total.to_csv(os.path.join(path_output, f"ev_consumption_ugr_2014_2022_by_energy_carrier.csv"))

def graph_ugr_consumption_historical():
    file_path = "src/utils/thesis_outputs/ev_consumption_ugr_historical/ev_consumption_ugr_2014_2022_by_energy_carrier.csv"
    df = pd.read_csv(file_path)

    # Rename columns to use natural English
    df.columns = [
        "Year", 
        "Biodiesel", 
        "Bioethanol", 
        "Biogas", 
        "Diesel", 
        "Liquefied Petroleum Gas", 
        "Natural Gas", 
        "Petrol", 
        "Electricity"
    ]

    # Convert year to string for plotting
    df["Year"] = df["Year"].astype(str)

    # Set index to year for plotting convenience
    df.set_index("Year", inplace=True)

    # Plot as stacked bar chart
    fig, ax = plt.subplots(figsize=(12, 6))
    df.plot(kind="bar", stacked=True, ax=ax)

    # Labeling
    ax.set_ylabel("Energy Consumption [MWh]")
    ax.set_xlabel("Year")

    # Set x-axis labels horizontal
    plt.xticks(rotation=0)

    # Legend at bottom
    ax.legend(title="Energy Carrier", loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=4)
    plt.tight_layout()

    save_plot_with_datetime(plt, path_output, "ev_consumption_ugr_2014_2022_by_energy_carrier", dpi=300)

#data_ugr_consumption_historical()
# graph_ugr_consumption_historical()



################### one week in 2018 for KBA_2 trend ######################################################
path_output = "src/utils/thesis_outputs/ev_consumption_temporal/"

def data_ev_consumption_temporal_2035_complete_year():
    df = electric_vehicle_consumption_by_region_id_and_temporal_resolution(year=2035, szenario="KBA_2", s2_szenario="trend", force_preprocessing=True)
    df.to_csv(os.path.join(path_output, "ev_consumption_temporal_kba_2_trend_2035.csv"))

def data_ev_consumption_temporal_2035_may():
    df = electric_vehicle_consumption_by_region_id_and_temporal_resolution(year=2035, szenario="KBA_2", s2_szenario="trend", force_preprocessing=True)
    df = electric_vehicle_consumption_by_region_id_and_temporal_resolution(year=2035, szenario="UGR" , force_preprocessing=True)

    df = df[df.index.month == 5]
    df.to_csv(os.path.join(path_output, "ev_consumption_temporal_ugr_2035_may.csv"))

def graph_ev_consumption_temporal_1h_1month():

    file_path = "src/utils/thesis_outputs/ev_consumption_temporal/ev_consumption_temporal_kba_2_trend_2035_may.csv"
    df_raw = pd.read_csv(file_path, header=[0, 1], index_col=0)
    df_raw.index = pd.to_datetime(df_raw.index)


    df_work = df_raw.loc[:, df_raw.columns.get_level_values(1) == "home_charging"]

    # Step 2: Sum across all regional_ids at each time step
    work_total = df_work.sum(axis=1)

    # Step 3: Resample to 1-hour total (summing 6 × 10-min intervals)
    work_hourly = work_total.resample("2H").sum()

    # Step 4: Plot
    plt.figure(figsize=(12, 5))
    plt.plot(work_hourly.index, work_hourly.values, label="Total Work Charging")
    plt.xlabel("Time")
    plt.ylabel("Electricity Consumption [MWh]")
    plt.title("Total Work Charging Load Over Time (1h Resolution)")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def graph_ev_consumption_temporal_1h_1week():

    #file_path = "src/utils/thesis_outputs/ev_consumption_temporal/ev_consumption_temporal_kba_2_trend_2035_may.csv"
    file_path = "src/utils/thesis_outputs/ev_consumption_temporal/ev_consumption_temporal_ugr_2035_may.csv"
    df_raw = pd.read_csv(file_path, header=[0, 1], index_col=0)
    df_raw.index = pd.to_datetime(df_raw.index)

    # Filter for the first week of May
    df = df_raw[(df_raw.index.month == 5) & (df_raw.index.day <= 7)]

    df_work = df.loc[:, df.columns.get_level_values(1) == "home_charging"]

    # Step 2: Sum across all regional_ids at each time step
    work_total = df_work.sum(axis=1)

    # Step 3: Resample to 1-hour total (summing 6 × 10-min intervals)
    work_hourly = work_total.resample("1H").sum()

    # Step 4: Plot
    plt.figure(figsize=(12, 5))
    plt.plot(work_hourly.index, work_hourly.values, label="Total Work Charging")
    plt.xlabel("Time")
    plt.ylabel("Electricity Consumption [MWh]")
    plt.title("Total Work Charging Load Over Time (1h Resolution)")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


#data_ev_consumption_temporal_2035_may()
graph_ev_consumption_temporal_1h_1week()