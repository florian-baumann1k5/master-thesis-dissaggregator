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
path_consumption = "src/utils/thesis_outputs/ev/ev_consumption/"
"""
this includes home, work and public charging!
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
        "KBA 1": "src/utils/thesis_outputs/ev/ev_consumption/ev_consumption_by_regional_id_kba_1_20250521_225507_20250521_225509.csv",
        "KBA 2 - Trend": "src/utils/thesis_outputs/ev/ev_consumption/ev_consumption_by_regional_id_kba_2_trend_20250521_224632_20250521_224910.csv",
        "KBA 2 - Regio": "src/utils/thesis_outputs/ev/ev_consumption/ev_consumption_by_regional_id_kba_2_regio_20250521_224632_20250521_225044.csv",
        "KBA 2 - Ambit": "src/utils/thesis_outputs/ev/ev_consumption/ev_consumption_by_regional_id_kba_2_ambit_20250521_224632_20250521_224835.csv",
        "UGR": "src/utils/thesis_outputs/ev/ev_consumption/ev_consumption_by_regional_id_ugr_20250521_220240_20250521_220242.csv"
    }

    # Plot setup
    fig, ax = plt.subplots(figsize=(12, 6))

    # Load and plot each scenario
    for label, path in files.items():
        df = pd.read_csv(path)
        df = df[df["year"].between(2017, 2022)]  # Filter for years 2017 to 2022
        df["total_consumption_million"] = df["total_consumption"] / 1e6
        ax.plot(df["year"], df["total_consumption_million"], label=label)

    # Formatting
    ax.set_xlabel("Year")
    ax.set_ylabel("Total Electric Vehicle Consumption [TWh]")

    # Legend outside
    ax.legend(loc='lower left', bbox_to_anchor=(1, 0))
    ax.grid(True)

    plt.tight_layout()

    save_plot_with_datetime(plt, path_consumption, "ev_consumption_total_2017_2022", dpi=300)

#data_total_ev_consumption_s2()
#data_total_ev_consumption()
#graph_ev_consumption()