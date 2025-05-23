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




################### map consumption cts & indsutry by regional_id 2022 ######################################################
path_output = "src/utils/thesis_outputs/petrol/consumption_by_regional_id"

#get_consumption_data_per_indsutry_sector_energy_carrier(year=2022, cts_or_industry="cts", energy_carrier="petrol", force_preprocessing=True)
#get_consumption_data(year=2020, energy_carrier="petrol", force_preprocessing=True)
#regional_energy_consumption_jevi = get_regional_energy_consumption(2022)

def graph_petrol_consumption_industry_2022():
    # Load EV data and normalize regional_id to string

    # Load the AGS to NUTS3 mapping file
    nuts3_map = pd.read_csv("src/utils/thesis_outputs/t_nuts3_lk.csv", dtype={"id_ags": str, "natcode_nuts3": str})
    nuts3_map["ags_5"] = nuts3_map["id_ags"].str[:-3]
    
    ev_df = pd.read_csv("data/processed/pipeline/consumption/consumption_data/con_2020_petrol.csv", index_col=0)
    # make all column names strings
    ev_df.columns = ev_df.columns.astype(str)

    # Transpose the dataframe to have regional_ids as index
    ev_df = ev_df.T
    # Create a new dataframe with the required columns
    transformed_df = pd.DataFrame(index=ev_df.index.rename("regional_id"))
    transformed_df['industry'] = ev_df.loc[:, 5:33].sum(axis=1)
    transformed_df['cts'] = ev_df.drop(columns=ev_df.loc[:, 5:33].columns).sum(axis=1)


    #sanity check
    if not np.isclose(transformed_df.sum().sum(), ev_df.sum().sum()):
        raise ValueError("The total consumption of petrol is not equal to the total consumption of petrol in the UGR")

    transformed_df["regional_id"] = transformed_df.index.astype(str)
    transformed_df["ags_5"] = transformed_df["regional_id"].str[:5]

    ev_df = transformed_df

    ev_df = ev_df.merge(nuts3_map[["ags_5", "natcode_nuts3"]], on="ags_5", how="left")
    ev_df.drop(columns=["regional_id", "ags_5"], inplace=True)
    ev_df.rename(columns={"natcode_nuts3": "nuts3"}, inplace=True)




    # Load the GeoJSON NUTS3 shapefile from Eurostat
    gdf = gpd.read_file("src/utils/thesis_outputs/NUTS_RG_20M_2024_4326.geojson")  # update path
    gdf = gdf[(gdf["LEVL_CODE"] == 3) & (gdf["CNTR_CODE"] == "DE")]

    merged_gdf = gdf.merge(ev_df, left_on="NUTS_ID", right_on="nuts3", how="left")
    merged_gdf["industry"] = merged_gdf["industry"].fillna(0)
    merged_gdf["cts"] = merged_gdf["cts"].fillna(0)
    merged_gdf["cts"] = merged_gdf["cts"] / 1e6
    merged_gdf["industry"] = merged_gdf["industry"] / 1e6

    # Plotting
    fig, ax = plt.subplots(1, 1, figsize=(10, 12))
    plot = merged_gdf.plot(
        column="industry",
        cmap="plasma",
        linewidth=0.6,
        edgecolor="black",
        ax=ax,
        legend=False
    )

    # Get colormap information for colorbar
    norm = plt.Normalize(vmin=merged_gdf["industry"].min(),
                        vmax=merged_gdf["industry"].max())
    sm = plt.cm.ScalarMappable(cmap="plasma", norm=norm)
    sm._A = []  # dummy array for colorbar

    # Add a colorbar right under the plot with reduced spacing
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("bottom", size="5%", pad=0.1)  # smaller pad
    cbar = fig.colorbar(sm, cax=cax, orientation="horizontal")
    cbar.set_label("Petrol Consumption Industry [TWh]")

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
    #plt.show()
    save_plot_with_datetime(plt, path_output, "petrol_consumption_industry_2022", dpi=300)

def graph_petrol_consumption_cts_2022():
    # Load EV data and normalize regional_id to string

    # Load the AGS to NUTS3 mapping file
    nuts3_map = pd.read_csv("src/utils/thesis_outputs/t_nuts3_lk.csv", dtype={"id_ags": str, "natcode_nuts3": str})
    nuts3_map["ags_5"] = nuts3_map["id_ags"].str[:-3]
    
    ev_df = pd.read_csv("data/processed/pipeline/consumption/consumption_data/con_2020_petrol.csv", index_col=0)
    # make all column names strings
    ev_df.columns = ev_df.columns.astype(str)

    # Transpose the dataframe to have regional_ids as index
    ev_df = ev_df.T
    # Create a new dataframe with the required columns
    transformed_df = pd.DataFrame(index=ev_df.index.rename("regional_id"))
    transformed_df['industry'] = ev_df.loc[:, 5:33].sum(axis=1)
    transformed_df['cts'] = ev_df.drop(columns=ev_df.loc[:, 5:33].columns).sum(axis=1)


    #sanity check
    if not np.isclose(transformed_df.sum().sum(), ev_df.sum().sum()):
        raise ValueError("The total consumption of petrol is not equal to the total consumption of petrol in the UGR")

    transformed_df["regional_id"] = transformed_df.index.astype(str)
    transformed_df["ags_5"] = transformed_df["regional_id"].str[:5]

    ev_df = transformed_df

    ev_df = ev_df.merge(nuts3_map[["ags_5", "natcode_nuts3"]], on="ags_5", how="left")
    ev_df.drop(columns=["regional_id", "ags_5"], inplace=True)
    ev_df.rename(columns={"natcode_nuts3": "nuts3"}, inplace=True)

    save_dataframe_with_datetime(ev_df, "petrol_consumption_cts_industry_2022", path_output)

    # Load the GeoJSON NUTS3 shapefile from Eurostat
    gdf = gpd.read_file("src/utils/thesis_outputs/NUTS_RG_20M_2024_4326.geojson")  # update path
    gdf = gdf[(gdf["LEVL_CODE"] == 3) & (gdf["CNTR_CODE"] == "DE")]

    merged_gdf = gdf.merge(ev_df, left_on="NUTS_ID", right_on="nuts3", how="left")
    merged_gdf["industry"] = merged_gdf["industry"].fillna(0)
    merged_gdf["cts"] = merged_gdf["cts"].fillna(0)

    # Convert MWh to TWh for plotting
    merged_gdf["cts"] = merged_gdf["cts"] / 1e6
    merged_gdf["industry"] = merged_gdf["industry"] / 1e6


    # Plotting
    fig, ax = plt.subplots(1, 1, figsize=(10, 12))
    plot = merged_gdf.plot(
        column="cts",
        cmap="plasma",
        linewidth=0.6,
        edgecolor="black",
        ax=ax,
        legend=False
    )

    # Get colormap information for colorbar
    norm = plt.Normalize(vmin=merged_gdf["cts"].min(),
                        vmax=merged_gdf["cts"].max())
    sm = plt.cm.ScalarMappable(cmap="plasma", norm=norm)
    sm._A = []  # dummy array for colorbar

    # Add a colorbar right under the plot with reduced spacing
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("bottom", size="5%", pad=0.1)  # smaller pad
    cbar = fig.colorbar(sm, cax=cax, orientation="horizontal")
    cbar.set_label("Petrol Consumption CTS [TWh]")

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
    #plt.show()
    save_plot_with_datetime(plt, path_output, "petrol_consumption_cts_2022", dpi=300)


def graph_power_consumption_industry_cts_2022():
    # Load EV data and normalize regional_id to string

    # Load the AGS to NUTS3 mapping file
    nuts3_map = pd.read_csv("src/utils/thesis_outputs/t_nuts3_lk.csv", dtype={"id_ags": str, "natcode_nuts3": str})
    nuts3_map["ags_5"] = nuts3_map["id_ags"].str[:-3]
    
    ev_df = pd.read_csv("data/processed/pipeline/consumption/consumption_data/con_2020_power.csv", index_col=0)
    # make all column names strings
    ev_df.columns = ev_df.columns.astype(str)
    
    # sort values of increasing regional_id
    ev_df = ev_df.sort_index(axis=1)
    
    # Transpose the dataframe to have regional_ids as index
    ev_df = ev_df.T
    # Create a new dataframe with the required columns
    transformed_df = pd.DataFrame(index=ev_df.index.rename("regional_id"))
    transformed_df['industry'] = ev_df.loc[:, 5:33].sum(axis=1)
    transformed_df['cts'] = ev_df.drop(columns=ev_df.loc[:, 5:33].columns).sum(axis=1)


    #sanity check
    if not np.isclose(transformed_df.sum().sum(), ev_df.sum().sum()):
        raise ValueError("The total consumption of petrol is not equal to the total consumption of petrol in the UGR")

    transformed_df["regional_id"] = transformed_df.index.astype(str)
    transformed_df["ags_5"] = transformed_df["regional_id"].str[:5]

    ev_df = transformed_df

    ev_df = ev_df.merge(nuts3_map[["ags_5", "natcode_nuts3"]], on="ags_5", how="left")
    ev_df.drop(columns=["regional_id", "ags_5"], inplace=True)
    ev_df.rename(columns={"natcode_nuts3": "nuts3"}, inplace=True)

    ev_def_export = ev_df.drop(columns=["nuts3"])
    save_dataframe_with_datetime(ev_def_export, "power_consumption_cts_industry_2022", path_output)


    # Load the GeoJSON NUTS3 shapefile from Eurostat
    gdf = gpd.read_file("src/utils/thesis_outputs/NUTS_RG_20M_2024_4326.geojson")  # update path
    gdf = gdf[(gdf["LEVL_CODE"] == 3) & (gdf["CNTR_CODE"] == "DE")]

    merged_gdf = gdf.merge(ev_df, left_on="NUTS_ID", right_on="nuts3", how="left")
    merged_gdf["industry"] = merged_gdf["industry"].fillna(0)
    merged_gdf["cts"] = merged_gdf["cts"].fillna(0)

    # Plotting
    fig, ax = plt.subplots(1, 1, figsize=(10, 12))
    plot = merged_gdf.plot(
        column="cts",
        cmap="plasma",
        linewidth=0.6,
        edgecolor="black",
        ax=ax,
        legend=False
    )

    # Get colormap information for colorbar
    norm = plt.Normalize(vmin=merged_gdf["cts"].min(),
                        vmax=merged_gdf["cts"].max())
    sm = plt.cm.ScalarMappable(cmap="plasma", norm=norm)
    sm._A = []  # dummy array for colorbar

    # Add a colorbar right under the plot with reduced spacing
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("bottom", size="5%", pad=0.1)  # smaller pad
    cbar = fig.colorbar(sm, cax=cax, orientation="horizontal")
    cbar.set_label("Power Consumption cts[MWh]")

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
    plt.show()
    #save_plot_with_datetime(plt, path_output, "power_consumption_cts_2022", dpi=300)



#graph_power_consumption_industry_cts_2022()
#graph_petrol_consumption_industry_2022()
#graph_petrol_consumption_cts_2022()
#graph_power_consumption_industry_2022()


################### candles consumption cts & indsutry by sector 2022 ######################################################
path_output = "src/utils/thesis_outputs/petrol/consumption_by_sector"



def graph_petrol_consumption_cts_sectors_2022():
    

    con_cts_sectors = pd.read_csv("data/processed/pipeline/consumption/consumption_data/con_2020_petrol.csv", index_col=0)


    sector_totals_mwh = con_cts_sectors.sum(axis=1)

    save_dataframe_with_datetime(sector_totals_mwh, "petrol_consumption_cts_industry_2022", path_output)

    # Step 2: Select top 7 sectors by consumption
    top7 = sector_totals_mwh.nlargest(7)

    # Step 3: Convert to TWh
    top7_twh = top7 / 1e6

    # Step 4: Plot
    plt.figure(figsize=(10, 6))
    top7_twh.plot(kind='bar', color='steelblue')

    # Labels
    plt.xlabel("Industry Sector")
    plt.ylabel("Petrol Consumption [TWh]")
    plt.xticks(rotation=0)
    plt.ylim(0, top7_twh.max() * 1.1)

    plt.tight_layout()
    save_plot_with_datetime(plt, path_output, "petrol_consumption_by_sectors_2022", dpi=300)




graph_petrol_consumption_cts_sectors_2022()






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