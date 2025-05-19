import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
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



path_graphs = "src/utils/thesis_outputs/graphs/"


# utils to save plots with datetime
def save_plot_with_datetime(plt_obj, name, dpi=300):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{now}.jpg"
    path = os.path.join(path_graphs, filename)
    plt_obj.savefig(path, dpi=dpi)
    plt_obj.show()




################### total consumption per year 2017-2045 ######################################################
path_consumption = "src/utils/thesis_outputs/ev_consumption/"
def data_total_ev_consumption():
    print("Done")
    df_total = pd.DataFrame()

    for year in range(2017, 2046):
        df = electric_vehicle_consumption_by_regional_id(year=year, szenario="KBA_2", s2_szenario="trend")
        total_consumption = df.sum().sum()
        df_total = pd.concat([df_total, pd.DataFrame({"year": [year], "total_consumption": [total_consumption]})])

    df_total.to_csv(os.path.join(path_consumption, "ev_consumption_by_regional_id_kba_2_trend.csv"), index=False)

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

# data_total_ev_consumption()
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
graph_ev_stock_historical()