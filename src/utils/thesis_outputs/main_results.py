import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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


path_graphs = "src/utils/thesis_outputs/graphs/"


# utils to save plots with datetime
def save_plot_with_datetime(plt_obj, name, dpi=300):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{now}.jpg"
    path = os.path.join(path_graphs, filename)
    plt_obj.savefig(path, dpi=dpi)
    plt_obj.show()

def save_dataframe_with_datetime(df, name):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{now}.csv"
    path = os.path.join(path_graphs, filename)
    df.to_csv(path)
    print(f"Saved {filename} to {path}")




################### total consumption per year 2017-2045 ######################################################
path_consumption = "src/utils/thesis_outputs/ev_consumption/"
"""
For kvb this also incluede public/home charging!
"""
def data_total_ev_consumption():
    print("Done")
    df_total = pd.DataFrame()

    for year in range(2017, 2046):
        df = electric_vehicle_consumption_by_regional_id(year=year, szenario="KBA_2", s2_szenario="trend")
        total_consumption = df.sum().sum()
        df_total = pd.concat([df_total, pd.DataFrame({"year": [year], "total_consumption": [total_consumption]})])

    save_dataframe_with_datetime(df_total, "ev_consumption_by_regional_id_kba_2_trend")


def graph_ev_consumption():
    # Load CSV files
    df1 = pd.read_csv(os.path.join(path_consumption, "ev_consumption_total_kba_1_2017_2045.csv"))
    df2 = pd.read_csv(os.path.join(path_consumption, "ev_consumption_total_kba_2_trend_2017_2045.csv"))
    df3 = pd.read_csv(os.path.join(path_consumption, "ev_consumption_total_ugr_2017_2045.csv"))


    # Convert MWh to TWh
    df1['total_consumption'] /= 1e6
    df2['total_consumption'] /= 1e6
    df3['total_consumption'] /= 1e6


    # Plot setup
    fig, ax = plt.subplots(figsize=(10, 5.625))

    # Plot data
    ax.plot(df1['year'], df1['total_consumption'], marker='o', markersize=4, label='KBA 1')
    ax.plot(df2['year'], df2['total_consumption'], marker='o', markersize=4, label='KBA 2 Trend')
    ax.plot(df3['year'], df3['total_consumption'], marker='o', markersize=4, label='UGR')

    # Set x-ticks to every year
    all_years = sorted(set(df1['year']) | set(df2['year']) | set(df3['year']))
    ax.set_xticks(all_years)

    # Labels
    ax.set_title("Energy consumption of electric vehicles")
    ax.set_xlabel("Year")
    ax.set_ylabel("[TWh]")

    # Legend outside
    ax.legend(loc='lower left', bbox_to_anchor=(1, 0))
    ax.grid(True)

    plt.tight_layout()

    save_plot_with_datetime(plt, "ev_consumption_total", dpi=300)

data_total_ev_consumption()
# graph_ev_consumption()




################### total ev stock 2017-2045 ######################################################
path_consumption = "src/utils/thesis_outputs/ev_stock_kba/"
def data_total_ev_stock_historical():

    df_total = pd.DataFrame()
    
    for year in range(2017, 2025):

        number_of_registered_evs = registered_electric_vehicles_by_regional_id(year=year)
        total_stock = number_of_registered_evs.sum().sum()
        df_total = pd.concat([df_total, pd.DataFrame({"year": [year], "total_stock": [total_stock]})])

    df_total.to_csv(os.path.join(path_consumption, "ev_stock_total_kba_2017_2024.csv"), index=False)

def data_total_ev_stock_future_s1():
    df_total = pd.DataFrame()
    for year in range(2025, 2046):
        number_of_registered_evs = s1_future_ev_stock_15mio_by_2030(year=year)
        total_stock = number_of_registered_evs
        df_total = pd.concat([df_total, pd.DataFrame({"year": [year], "total_stock": [total_stock]})])

    df_total.to_csv(os.path.join(path_consumption, "ev_stock_total_kba_1_2025_2045.csv"), index=False)

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
            data[szenario].append(number_of_registered_evs)

    df_total = pd.DataFrame(data)
    df_total.to_csv(os.path.join(path_consumption, "ev_stock_total_kba_2_2025_2045.csv"), index=False)


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
    
def graph_ev_stock_complete():
    # Load data
    df_hist = pd.read_csv("src/utils/thesis_outputs/ev_stock_kba/ev_stock_total_kba_2017_2024.csv")
    df_kba1 = pd.read_csv("src/utils/thesis_outputs/ev_stock_kba/ev_stock_total_kba_1_2025_2045.csv")
    df_kba2 = pd.read_csv("src/utils/thesis_outputs/ev_stock_kba/ev_stock_total_kba_2_2025_2045.csv")
    
    df_hist['total_stock'] /= 1e6
    df_kba1['total_stock'] /= 1e6
    df_kba2['trend'] /= 1e6
    df_kba2['ambit'] /= 1e6
    df_kba2['regio'] /= 1e6

    # Create figure
    fig, ax = plt.subplots(figsize=(10, 5.625))


    ax.plot(df_hist['year'], df_hist['total_stock'], marker='o', markersize=4, label='Historisch')
    ax.plot(df_kba1['year'], df_kba1['total_stock'], marker='o', markersize=4, label='KBA 1')
    ax.plot(df_kba2['year'], df_kba2['trend'], marker='o', markersize=4, label='KBA 2 Trend')
    ax.plot(df_kba2['year'], df_kba2['ambit'], marker='o', markersize=4, label='KBA 2 Ambitioniert')
    ax.plot(df_kba2['year'], df_kba2['regio'], marker='o', markersize=4, label='KBA 2 Regional')


    # x-axis: yearly ticks
    all_years = list(range(df_hist['year'].min(), df_kba2['year'].max() + 1))
    ax.set_xticks(all_years)

    # Labels
    ax.set_title("EV Stock – Historical & Scenarios")
    ax.set_xlabel("Year")
    ax.set_ylabel("Mio EVs")

    # Grid, legend, layout
    ax.legend(loc='lower left', bbox_to_anchor=(1, 0))
    ax.grid(True)
    plt.tight_layout()

    save_plot_with_datetime(plt, "ev_stock_complete", dpi=300)

# data_total_ev_stock_future_s1()
# graph_ev_stock_complete()
#graph_ev_stock_historical()


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


#df = electric_vehicle_consumption_by_region_id_and_temporal_resolution(year=2024, szenario="KBA_2", s2_szenario="trend")
#print(df)