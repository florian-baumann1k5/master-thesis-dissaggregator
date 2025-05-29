import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import geopandas as gpd
from matplotlib.patches import Rectangle
from mpl_toolkits.axes_grid1 import make_axes_locatable
import pandas as pd
import os



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




################### map consumption cts & indsutry by regional_id 2020 ######################################################
path_output = "src/utils/thesis_outputs/petrol/consumption_by_regional_id"

#get_consumption_data_per_indsutry_sector_energy_carrier(year=2020, cts_or_industry="cts", energy_carrier="petrol", force_preprocessing=True)
#get_consumption_data(year=2020, energy_carrier="petrol", force_preprocessing=True)
#regional_energy_consumption_jevi = get_regional_energy_consumption(2020)

def graph_petrol_consumption_industry_2020():
    # Load EV data and normalize regional_id to string

    # Load the AGS to NUTS3 mapping file
    nuts3_map = pd.read_csv("src/utils/thesis_outputs/t_nuts3_lk.csv", dtype={"id_ags": str, "natcode_nuts3": str})
    nuts3_map["ags_5"] = nuts3_map["id_ags"].str[:-3]
    
    ev_df = pd.read_csv("data/output/consumption/consumption_data/con_2020_petrol.csv", index_col=0)
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
    cbar.set_label("Petrol Consumption Industry [TWh]", fontsize=14)

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
    save_plot_with_datetime(plt, path_output, "petrol_consumption_industry_2020", dpi=300)

def graph_petrol_consumption_cts_2020():
    # Load EV data and normalize regional_id to string

    # Load the AGS to NUTS3 mapping file
    nuts3_map = pd.read_csv("src/utils/thesis_outputs/t_nuts3_lk.csv", dtype={"id_ags": str, "natcode_nuts3": str})
    nuts3_map["ags_5"] = nuts3_map["id_ags"].str[:-3]
    
    ev_df = pd.read_csv("data/output/consumption/consumption_data/con_2020_petrol.csv", index_col=0)
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

    save_dataframe_with_datetime(ev_df, "petrol_consumption_cts_industry_2020", path_output)

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
    cbar.set_label("Petrol Consumption CTS [TWh]", fontsize=14)

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
    save_plot_with_datetime(plt, path_output, "petrol_consumption_cts_2020", dpi=300)

def graph_petrol_consumption_cts_industry_2020():
    # Load EV data and normalize regional_id to string

    # Load the AGS to NUTS3 mapping file
    nuts3_map = pd.read_csv("src/utils/thesis_outputs/t_nuts3_lk.csv", dtype={"id_ags": str, "natcode_nuts3": str})
    nuts3_map["ags_5"] = nuts3_map["id_ags"].str[:-3]
    
    ev_df = pd.read_csv("data/output/consumption/consumption_data/con_2020_petrol.csv", index_col=0)
    # make all column names strings
    ev_df.columns = ev_df.columns.astype(str)

    # Transpose the dataframe to have regional_ids as index
    ev_df = ev_df.T
    # Sum all columns to get total consumption
    ev_df['total_consumption'] = ev_df.sum(axis=1)

    # Sanity check
    
    ev_df["regional_id"] = ev_df.index.astype(str)
    ev_df["ags_5"] = ev_df["regional_id"].str[:5]

    ev_df = ev_df.merge(nuts3_map[["ags_5", "natcode_nuts3"]], on="ags_5", how="left")
    ev_df.drop(columns=["regional_id", "ags_5"], inplace=True)
    ev_df.rename(columns={"natcode_nuts3": "nuts3"}, inplace=True)

    save_dataframe_with_datetime(ev_df, "petrol_consumption_total_2020", path_output)

    # Load the GeoJSON NUTS3 shapefile from Eurostat
    gdf = gpd.read_file("src/utils/thesis_outputs/NUTS_RG_20M_2024_4326.geojson")  # update path
    gdf = gdf[(gdf["LEVL_CODE"] == 3) & (gdf["CNTR_CODE"] == "DE")]

    merged_gdf = gdf.merge(ev_df, left_on="NUTS_ID", right_on="nuts3", how="left")
    merged_gdf["total_consumption"] = merged_gdf["total_consumption"].fillna(0)

    # Convert MWh to TWh for plotting
    merged_gdf["total_consumption"] = merged_gdf["total_consumption"] / 1e6

    # Plotting
    fig, ax = plt.subplots(1, 1, figsize=(10, 12))
    plot = merged_gdf.plot(
        column="total_consumption",
        cmap="plasma",
        linewidth=0.6,
        edgecolor="black",
        ax=ax,
        legend=False
    )

    # Get colormap information for colorbar
    norm = plt.Normalize(vmin=merged_gdf["total_consumption"].min(),
                        vmax=merged_gdf["total_consumption"].max())
    sm = plt.cm.ScalarMappable(cmap="plasma", norm=norm)
    sm._A = []  # dummy array for colorbar

    # Add a colorbar right under the plot with reduced spacing
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("bottom", size="5%", pad=0.1)  # smaller pad
    cbar = fig.colorbar(sm, cax=cax, orientation="horizontal")
    cbar.set_label("Petrol Consumption for industry and CTS sector [TWh]", fontsize=14)

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
    save_plot_with_datetime(plt, path_output, "petrol_consumption_cts_industry_2020", dpi=300)


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


#graph_petrol_consumption_cts_industry_2020()




################### candles consumption cts & indsutry by sector 2022 ######################################################
path_output = "src/utils/thesis_outputs/petrol/consumption_by_sector"

def graph_petrol_consumption_cts_sectors_2020():
    

    con_cts_sectors = pd.read_csv("data/output/consumption/consumption_data/con_2020_petrol.csv", index_col=0)


    sector_totals_mwh = con_cts_sectors.sum(axis=1)

    save_dataframe_with_datetime(sector_totals_mwh, "petrol_consumption_cts_industry_2020", path_output)

    # Step 2: Select top 7 sectors by consumption
    top7 = sector_totals_mwh.nlargest(7)

    # Step 3: Convert to TWh
    top7_twh = top7 / 1e6

    # Step 4: Plot
    plt.figure(figsize=(10, 4))
    top7_twh.plot(kind='bar', color='steelblue')

    # Labels
    plt.xlabel("Industry Sector", fontsize=14)
    plt.ylabel("Petrol Consumption [TWh]", fontsize=14)
    plt.xticks(rotation=0, fontsize=12)
    plt.ylim(0, top7_twh.max() * 1.1)

    plt.tight_layout()
    save_plot_with_datetime(plt, path_output, "petrol_consumption_by_sectors_2020", dpi=300)


#graph_petrol_consumption_cts_sectors_2020()


################### candles applications petrol for CTS and industry 2022 ######################################################
path_output = "src/utils/thesis_outputs/petrol/applications"
#data/output/applications/disagg_applications_efficiency_factor/con_eff_2020_cts_petrol.csv
#data/output/applications/disagg_applications_efficiency_factor/con_eff_2020_industry_petrol.csv
#df1 = get_application_dissaggregation_factors(sector="industry", energy_carrier="petrol")

""" find highest consumption in df
max_value = df.max().max()

location = df == max_value
index_tuple = location.stack(future_stack=True).idxmax()

row_index = index_tuple[0]
column_index = index_tuple[1]
"""

def graph_petrol_applications_cts_industry_2020():

    def load_and_aggregate_multicolumn(filepath: str) -> pd.Series:
        """
        Loads a CSV with multi-index columns: [industry_sector, application],
        sums over all regional_ids, then aggregates by application.
        Returns consumption in TWh per application.
        """
        # Read with MultiIndex columns
        df = pd.read_csv(filepath, header=[0, 1], index_col=0)
        
        # Step 1: Sum over all regional_ids
        summed = df.sum(axis=0)

        # Step 2: Group by application (level 1 of columns)
        application_sums = summed.groupby(level=1).sum()

        # Step 3: Convert to TWh
        return application_sums / 1e6

    # Example usage
    cts_path = "data/output/applications/disagg_applications_efficiency_factor/con_eff_2020_cts_petrol.csv"
    industry_path = "data/output/applications/disagg_applications_efficiency_factor/con_eff_2020_industry_petrol.csv"

    cts_series = load_and_aggregate_multicolumn(cts_path)
    industry_series = load_and_aggregate_multicolumn(industry_path)

    # Combine and plot
    df_combined = pd.DataFrame({
        "CTS": cts_series,
        "Industry": industry_series
    }).fillna(0)

    # Plotting
    import matplotlib.pyplot as plt
    df_combined.plot(kind="bar", figsize=(12, 6))
    plt.xlabel("Application")
    plt.ylabel("Petrol Consumption [TWh]")
    plt.xticks(ha='right')
    plt.tight_layout()
    save_plot_with_datetime(plt, path_output, "petrol_applications_cts_industry_2020", dpi=300)
    

def graph_petrol_applications_cts_2020():

    def load_and_aggregate_cts(filepath: str) -> pd.Series:
        """
        Loads CTS CSV with MultiIndex columns [industry_sector, application],
        sums over all regional_ids and industry sectors,
        and returns total consumption per application in TWh.
        """
        df = pd.read_csv(filepath, header=[0, 1], index_col=0)
        
        # Rename columns to normal English
        df.columns = df.columns.set_levels([
            'Hot Water', 'Mechanical Energy', 'Non-Energetic Use', 'Process Heat', 'Space Heating'
        ], level=1)
        
        summed = df.sum(axis=0)
        application_sums = summed.groupby(level=1).sum()
        return application_sums / 1e6  # Convert to TWh

    # Load and process CTS data
    cts_path = "data/output/applications/disagg_applications_efficiency_factor/con_eff_2020_cts_petrol.csv"
    cts_series = load_and_aggregate_cts(cts_path)

    # Plot
    plt.figure(figsize=(10, 6))
    cts_series.sort_values(ascending=False).plot(kind="bar", color="steelblue")
    plt.ylabel("Petrol products consumption [TWh]", fontsize=14)
    plt.xticks(rotation=45, ha='right', fontsize=14)
    plt.tight_layout()
    save_plot_with_datetime(plt, path_output, "petrol_applications_cts_2020", dpi=300)

def graph_petrol_applications_industry_2020():
    def load_and_aggregate_industry(filepath: str) -> pd.Series:
        """
        Loads Industry CSV with MultiIndex columns [industry_sector, application],
        sums over all regional_ids and industry sectors,
        and returns total consumption per application in TWh.
        """
        df = pd.read_csv(filepath, header=[0, 1], index_col=0)
        
        # Rename columns to normal English
        df.columns = df.columns.set_levels([
            'Hot Water', 'Mechanical Energy', 'Non-Energetic Use', 
            'Process Heat 100 to 200C', 'Process Heat 200 to 500C', 
            'Process Heat Above 500C', 'Process Heat Below 100C', 
            'Space Heating'
        ], level=1)
        
        summed = df.sum(axis=0)
        application_sums = summed.groupby(level=1).sum()
        return application_sums / 1e6  # Convert to TWh

    # Load and process Industry data
    industry_path = "data/output/applications/disagg_applications_efficiency_factor/con_eff_2020_industry_petrol.csv"
    industry_series = load_and_aggregate_industry(industry_path)

    # Plot
    plt.figure(figsize=(10, 6))
    industry_series.sort_values(ascending=False).plot(kind="bar", color="darkgreen")
    plt.ylabel("Industry Petrol Consumption [TWh]", fontsize=16)
    plt.xticks(rotation=45, ha='right', fontsize=14)
    plt.yticks(fontsize=14)
    plt.tight_layout()
    save_plot_with_datetime(plt, path_output, "petrol_applications_industry_2020", dpi=300)

def graph_petrol_applications_industry_bysector_2022():
    def load_industry_stacked(filepath: str) -> pd.DataFrame:
        """
        Loads industry CSV, returns a DataFrame indexed by application,
        with columns as industry sectors and values as total consumption in TWh.
        """
        df = pd.read_csv(filepath, header=[0, 1], index_col=0)
        
        # Step 1: Sum across regional_ids
        summed = df.sum(axis=0)  # MultiIndex: (sector, application)
        
        # Step 2: Convert to TWh
        summed = summed / 1e6
        
        # Step 3: Unstack to get DataFrame: rows = application, cols = sectors
        df_stacked = summed.unstack(level=0).fillna(0)

        return df_stacked

    # Load and process
    industry_path = "data/processed/pipeline/applications/disagg_applications_efficiency_factor/con_eff_2022_industry_petrol.csv"
    df_plot = load_industry_stacked(industry_path)

    # Plot stacked bar chart
    df_plot.plot(kind="bar", stacked=True, figsize=(12, 6), colormap="tab20")
    plt.xlabel("Application")
    plt.ylabel("Electricity Consumption [TWh]")
    plt.title("Industry Electricity Consumption by Application and Sector (2022, Petrol)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    #plt.savefig("industry_application_stacked_by_sector.png", dpi=300)
    plt.show()

#graph_petrol_applications_cts_2020()


################### temporal for CTS and industry 2022 ######################################################
path_output = "src/utils/thesis_outputs/petrol/temporal"


#df1 = disaggregate_temporal(sector="cts", energy_carrier="petrol", year=2022, force_preprocessing=True)
#print("fertig1")

#df2 = disaggregate_temporal(sector="industry", energy_carrier="petrol", year=2022, force_preprocessing=True)
#print("fertig2")

#print("fertig")

def data_petrol_temporal_cts_2020():
    # === Step 1: Load the Data ===
    cts_file = "data/output/temporal/consumption_disaggregate_temporal/con_disagg_temp_2020_cts_petrol.csv"
    industry_file = "data/output/temporal/consumption_disaggregate_temporal/con_disagg_temp_2020_industry_petrol.csv"
    
    df_cts = pd.read_csv(cts_file, header=[0, 1], index_col=0)
    df_industry = pd.read_csv(industry_file, header=[0, 1], index_col=0)
    
    df_cts.index = pd.to_datetime(df_cts.index)
    df_industry.index = pd.to_datetime(df_industry.index)

    # === Step 2: Sum across all regional_ids ===
    df_cts_summed = df_cts.groupby(axis=1, level=1).sum()  # result: columns = industry_sector, index = time
    df_industry_summed = df_industry.groupby(axis=1, level=1).sum()

    # === Step 3: Combine CTS and Industry Data ===
    df_combined = pd.DataFrame(index=df_cts_summed.index)
    df_combined["cts"] = df_cts_summed.sum(axis=1)
    df_combined["industry"] = df_industry_summed.sum(axis=1)

    # === Step 4: Filter May and Resample to 2h ===
    df_may = df_combined.loc[df_combined.index.month == 5]
    df_may_2h = df_may.resample("2H").sum()

    # === Step 5: Save to file ===
    save_dataframe_with_datetime(df_may_2h, "petrol_consumption_cts_industry_may_2020_2h", path_output)

    # === Step 6: Plot ===
    plt.figure(figsize=(12, 6))
    plt.plot(df_may_2h.index, df_may_2h["industry"], label="Industry", color="tab:blue")
    plt.plot(df_may_2h.index, df_may_2h["cts"], label="CTS", color="tab:orange")

    plt.ylabel("Petrol Consumption [MWh]", fontsize=14)
    plt.xticks(rotation=45, ha='right', fontsize=14)
    plt.yticks(fontsize=14)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    save_plot_with_datetime(plt, path_output, "petrol_consumption_cts_industry_may_2020_2h", dpi=300)

def data_petrol_temporal_cts_2020_weekly():
    # === Step 1: Load the Data ===
    cts_file = "data/output/temporal/consumption_disaggregate_temporal/con_disagg_temp_2020_cts_petrol.csv"
    industry_file = "data/output/temporal/consumption_disaggregate_temporal/con_disagg_temp_2020_industry_petrol.csv"
    
    df_cts = pd.read_csv(cts_file, header=[0, 1], index_col=0)
    df_industry = pd.read_csv(industry_file, header=[0, 1], index_col=0)
    
    df_cts.index = pd.to_datetime(df_cts.index)
    df_industry.index = pd.to_datetime(df_industry.index)

    # === Step 2: Sum across all regional_ids ===
    df_cts_summed = df_cts.groupby(axis=1, level=1).sum()  # result: columns = industry_sector, index = time
    df_industry_summed = df_industry.groupby(axis=1, level=1).sum()

    # === Step 3: Combine CTS and Industry Data ===
    df_combined = pd.DataFrame(index=df_cts_summed.index)
    df_combined["cts"] = df_cts_summed.sum(axis=1)
    df_combined["industry"] = df_industry_summed.sum(axis=1)

    # === Step 4: Filter the first week of May and Resample to 2h ===
    df_first_week_may = df_combined.loc[(df_combined.index.month == 5) & (df_combined.index.day <= 7)]
    df_first_week_may_2h = df_first_week_may.resample("2H").sum()

    # === Step 5: Save to file ===
    save_dataframe_with_datetime(df_first_week_may_2h, "petrol_consumption_cts_industry_first_week_may_2020_2h", path_output)

    # === Step 6: Plot ===
    plt.figure(figsize=(12, 6))
    plt.plot(df_first_week_may_2h.index, df_first_week_may_2h["industry"], label="Industry", color="tab:blue")
    plt.plot(df_first_week_may_2h.index, df_first_week_may_2h["cts"], label="CTS", color="tab:orange")

    plt.ylabel("Petrol Consumption [MWh]", fontsize=14)
    plt.xticks(rotation=45, ha='right', fontsize=14)
    plt.yticks(fontsize=14)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    save_plot_with_datetime(plt, path_output, "petrol_consumption_cts_industry_first_week_may_2020_2h", dpi=300)


def data_petrol_temporal_cts_2022_daily():
    # === Step 1: Load the Data ===
    cts_file = "data/processed/pipeline/temporal/consumption_disaggregate_temporal/con_disagg_temp_2022_cts_petrol.csv"
    industry_file = "data/processed/pipeline/temporal/consumption_disaggregate_temporal/con_disagg_temp_2022_industry_petrol.csv"
    
    df_cts = pd.read_csv(cts_file, header=[0, 1], index_col=0)
    df_industry = pd.read_csv(industry_file, header=[0, 1], index_col=0)
    
    df_cts.index = pd.to_datetime(df_cts.index)
    df_industry.index = pd.to_datetime(df_industry.index)

    # === Step 2: Sum across all regional_ids ===
    df_cts_summed = df_cts.groupby(axis=1, level=1).sum()  # result: columns = industry_sector, index = time
    df_industry_summed = df_industry.groupby(axis=1, level=1).sum()

    # === Step 3: Combine CTS and Industry Data ===
    df_combined = pd.DataFrame(index=df_cts_summed.index)
    df_combined["cts"] = df_cts_summed.sum(axis=1)
    df_combined["industry"] = df_industry_summed.sum(axis=1)

    # === Step 4: Resample to daily ===
    df_daily = df_combined.resample("D").sum()

    # === Step 5: Save to file ===
    save_dataframe_with_datetime(df_daily, "petrol_consumption_cts_industry_year_2022_daily", path_output)

    # === Step 6: Plot ===
    plt.figure(figsize=(12, 6))
    plt.plot(df_daily.index, df_daily["industry"], label="Industry", color="tab:blue")
    plt.plot(df_daily.index, df_daily["cts"], label="CTS", color="tab:orange")

    plt.xlabel("Time (2022)")
    plt.ylabel("Petrol Consumption [MWh]")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    save_plot_with_datetime(plt, path_output, "petrol_consumption_cts_industry_year_2022_daily", dpi=300)

#data_petrol_temporal_cts_2020_weekly()



################### sector_fuel_switch_fom for CTS and industry 2022 ######################################################
path_output = "src/utils/thesis_outputs/petrol/heat/sector_fuel_switch"

# for year in [ 2025, 2030, 2035, 2040, 2045]:
#     for sector in ["cts", "industry"]:
#         df = disaggregate_temporal(sector=sector, energy_carrier="petrol", year=year, force_preprocessing=True, float_precision=8)


def data_sector_fuel_switch_fom_gas_petrol_cts_power():
    for year in [ 2025, 2030, 2035, 2040, 2045]:

        print(f"Processing year: {year}")
        def1 = sector_fuel_switch_fom_gas_petrol(year=year, sector="cts", energy_carrier="petrol", switch_to="power", force_preprocessing = True)

        save_dataframe_with_datetime(def1, f"sector_fuel_switch_fom_gas_petrol_cts_power_{year}", path_output)

def data_sector_fuel_switch_fom_gas_petrol_industry_power():
    for year in [ 2025, 2030, 2035, 2040, 2045]:

        print(f"Processing year: {year}")
        def1 = sector_fuel_switch_fom_gas_petrol(year=year, sector="industry", energy_carrier="petrol", switch_to="power", force_preprocessing = True)

        save_dataframe_with_datetime(def1, f"sector_fuel_switch_fom_gas_petrol_industry_power_{year}", path_output)

def data_sector_fuel_switch_fom_gas_petrol_industry_hydrogen():
    for year in [ 2025, 2030, 2035, 2040, 2045]:

        print(f"Processing year: {year}")
        def1 = sector_fuel_switch_fom_gas_petrol(year=year, sector="industry", energy_carrier="petrol", switch_to="hydrogen", force_preprocessing = True)

        save_dataframe_with_datetime(def1, f"sector_fuel_switch_fom_gas_petrol_industry_hydrogen_{year}", path_output)


def combine_and_save_dataframes(years, base_path, output_path):
    for year in years:
        # Load CTS and Industry dataframes
        cts_file = os.path.join(base_path, f"sector_fuel_switch_fom_gas_petrol_cts_power_{year}.csv")
        industry_file = os.path.join(base_path, f"sector_fuel_switch_fom_gas_petrol_industry_power_{year}.csv")
        
        df_cts = pd.read_csv(cts_file, header=[0, 1], index_col=0)
        df_industry = pd.read_csv(industry_file, header=[0, 1], index_col=0)
        
        # Sum up regional_ids and economic sectors
        df_cts_summed = df_cts.groupby(level=1, axis=1).sum().sum(axis=0)
        df_industry_summed = df_industry.groupby(level=1, axis=1).sum().sum(axis=0)
        
        # Add the CTS and Industry dataframes
        df_combined = df_cts_summed.add(df_industry_summed, fill_value=0)
        
        # Save the result in a new CSV
        combined_file_path = os.path.join(output_path, f"sector_fuel_switch_fom_gas_petrol_combined_power_{year}.csv")
        df_combined.to_csv(combined_file_path)
        print(f"Combined data for year {year} saved to {combined_file_path}")

# years = [2025, 2030, 2035, 2040, 2045]
# base_path = "src/utils/thesis_outputs/petrol/heat/sector_fuel_switch"
# output_path = "src/utils/thesis_outputs/petrol/heat/combined"
# os.makedirs(output_path, exist_ok=True)

# combine_and_save_dataframes(years, base_path, output_path)

def graph_sector_fuel_switch_fom_gas_petrol_combined_power():
    years = list(range(2025, 2050, 5))
    base_path = "src/utils/thesis_outputs/petrol/heat/combined"
    file_template = "sector_fuel_switch_fom_gas_petrol_combined_power_{}.csv"

    # Collect data
    data = {}

    for year in years:
        file_path = os.path.join(base_path, file_template.format(year))
        df = pd.read_csv(file_path, index_col=0)
        df.columns = ["value"]
        # Convert MWh to TWh
        df["value"] = df["value"] / 1e6
        data[year] = df["value"]

    # Combine into a single DataFrame
    df_all = pd.DataFrame(data).T  # years as rows, applications as columns

    # Sort applications by their size in the last year
    df_all = df_all[df_all.columns[df_all.loc[2045].argsort()[::-1]]]

    rename_map = {
        "hot_water": "Hot Water",
        "mechanical_energy": "Mechanical Energy",
        "process_heat": "Process Heat",
        "process_heat_below_100C": "Process Heat < 100°C",
        "process_heat_100_to_200C": "Process Heat 100–200°C",
        "process_heat_200_to_500C": "Process Heat 200–500°C",
        "space_heating": "Space Heating",
    }

    df_all.rename(columns=rename_map, inplace=True)

    # Plotting
    fig, ax = plt.subplots(figsize=(10, 6))
    bottom = pd.Series([0] * len(df_all), index=df_all.index)
    colors = plt.cm.tab20.colors

    for i, column in enumerate(df_all.columns):
        ax.bar(df_all.index, df_all[column], bottom=bottom, color=colors[i], label=column)
        bottom += df_all[column]

    # Formatting
    ax.set_xlabel("Year", fontsize=14)
    ax.set_ylabel("Total Consumption [TWh/year]", fontsize=14)
    ax.legend(title="Application", bbox_to_anchor=(1.05, 0), loc="lower left", fontsize=14, title_fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()

    # Save and Show
    save_plot_with_datetime(plt, path_output, "sector_fuel_switch_fom_cts_power_petrol_combined_bar", dpi=300)
    plt.show()

graph_sector_fuel_switch_fom_gas_petrol_combined_power()

def graph_sector_fuel_switch_fom_gas_petrol_cts_power():
    years = [ 2025, 2030, 2035, 2040, 2045]
    base_path = "data/processed/heat/temporal_fuel_switch"
    file_template = "sector_fuel_switch_{}_cts_power_petrol.csv"

    # Load and process data
    application_data = {}

    for year in years:
        file_path = os.path.join(base_path, file_template.format(year))
        df = pd.read_csv(file_path, header=[0, 1], index_col=0)
        df.columns.names = ['industry_sector', 'application']
        
        # Sum over all regional_ids, and group by application
        df_year = df.groupby(level=1, axis=1).sum().sum(axis=0)
        application_data[year] = df_year
    
    df_application_data = pd.DataFrame(application_data)
    save_dataframe_with_datetime(df_application_data, "sector_fuel_switch_fom_gas_petrol_cts_power_2020_2045", path_output)

    # Convert to DataFrame
    df_plot = pd.DataFrame(application_data).T  # Transpose to get years on x-axis

    # Plot
    ax = df_plot.plot(kind='bar', stacked=True, figsize=(12, 6))

    plt.title("Total Consumption by Application (CTS Power Petrol)")
    plt.xlabel("Year")
    plt.ylabel("Total Consumption (e.g., GWh or as in input)")
    plt.legend(title="Application", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    #plt.savefig("src/utils/thesis_outputs/cts_power_petrol_application_stackedbar.png", dpi=300)
    plt.show()


# data_sector_fuel_switch_fom_gas_petrol_cts_power()
# data_sector_fuel_switch_fom_gas_petrol_industry_power()
# data_sector_fuel_switch_fom_gas_petrol_industry_hydrogen()
print("x")




################### heat for CTS and industry 2022 ######################################################
path_output = "src/utils/thesis_outputs/petrol/heat"

## CTS
def temporal_cts_elec_load_from_fuel_switch_petrol_power():
    for year in [2020, 2025, 2030, 2035, 2040, 2045]:

        print(f"Processing year: {year}")

        total_cts = pd.DataFrame()

        for state in federal_state_dict().values():
            def1 = temporal_cts_elec_load_from_fuel_switch_petrol(year=year, state=state, switch_to="power")
            def1 = def1.groupby(level='application', axis=1).sum()
            
            total_cts = pd.concat([total_cts, def1], axis=1)

        save_dataframe_with_datetime(total_cts, f"temporal_cts_elec_load_from_fuel_switch_petrol{year}_power", path_output)

#temporal_cts_elec_load_from_fuel_switch_petrol_power()


def graph_temporal_cts_elec_load_from_fuel_switch_petrol_power(): # passt
    files = {
        2025: "src/utils/thesis_outputs/petrol/heat/temporal_cts_elec_load_from_fuel_switch_petrol2025_power_20250525_203110.csv",
        2030: "src/utils/thesis_outputs/petrol/heat/temporal_cts_elec_load_from_fuel_switch_petrol2030_power_20250525_213822.csv",
        2035: "src/utils/thesis_outputs/petrol/heat/temporal_cts_elec_load_from_fuel_switch_petrol2035_power_20250525_224546.csv",
        2040: "src/utils/thesis_outputs/petrol/heat/temporal_cts_elec_load_from_fuel_switch_petrol2040_power_20250525_235224.csv",
        2045: "src/utils/thesis_outputs/petrol/heat/temporal_cts_elec_load_from_fuel_switch_petrol2045_power_20250526_090846.csv",
    }

    # Dictionary to collect yearly totals per application
    yearly_data = {}

    for year, path in files.items():
        df = pd.read_csv(path, index_col=0)

        # Clean duplicated column names (e.g., "process_heat.1" → "process_heat")
        df.columns = df.columns.str.replace(r"\.\d+$", "", regex=True)

        # Group by application name (sum duplicates)
        df_grouped = df.groupby(df.columns, axis=1).sum()

        # Sum across all time steps to get total consumption per application
        # Convert from MWh to TWh by dividing by 1,000,000
        yearly_data[year] = df_grouped.sum() / 1_000_000

    # Combine all into one DataFrame
    df_all_years = pd.DataFrame(yearly_data).T  # rows = years, cols = applications

    # Plotting
    fig, ax = plt.subplots(figsize=(10, 6))
    bottom = pd.Series([0] * len(df_all_years), index=df_all_years.index)
    colors = plt.cm.tab20.colors

    for i, column in enumerate(df_all_years.columns):
        ax.bar(df_all_years.index, df_all_years[column], bottom=bottom, color=colors[i], label=column)
        bottom += df_all_years[column]

    # Formatting
    ax.set_xlabel("Year", fontsize=14)
    ax.set_ylabel("Total Electricity Consumption [TWh/year]", fontsize=14)
    ax.legend(title="Application", bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=14)
    plt.tight_layout()
    plt.grid(axis='y', linestyle='--', alpha=0.6)

    # Save and Show
    save_plot_with_datetime(plt, path_output, "cts_power_petrol_bar_all_years", dpi=300)

#graph_temporal_cts_elec_load_from_fuel_switch_petrol_power()


## Industry
def data_temporal_temporal_industry_elec_load_from_fuel_switch_petrol_power():
    for year in [2020, 2025, 2030, 2035, 2040, 2045]:

        print(f"Processing year: {year}")

        total_cts = pd.DataFrame()

        for state in federal_state_dict().values():
            def1 = temporal_industry_elec_load_from_fuel_switch_petrol(year=year, state=state, switch_to="power")
            def1 = def1.groupby(level='application', axis=1).sum()
            
            total_cts = pd.concat([total_cts, def1], axis=1)

        save_dataframe_with_datetime(total_cts, f"temporal_industry_elec_load_from_fuel_switch_petrol{year}_power", path_output)

def data_hydrogen():
    for year in [2020, 2025, 2030, 2035, 2040, 2045]:

        print(f"Processing year: {year}")

        total_cts = pd.DataFrame()

        for state in federal_state_dict().values():
            def1 = hydrogen(year=year, state=state, energy_carrier="petrol")
            def1 = def1.groupby(level='application', axis=1).sum()
            
            total_cts = pd.concat([total_cts, def1], axis=1)

        save_dataframe_with_datetime(total_cts, f"temporal_industrydata_hydrogen{year}_hydrogen", path_output)

#temporal_temporal_industry_elec_load_from_fuel_switch_petrol_power()
#data_hydrogen()



#graph_temporal_cts_elec_load_from_fuel_switch_petrol_power()


"""this will retrun a df with:
index: daytime
column[0]: regional_ids
column[1]: industry_sectors
column[2]: applications
"""



