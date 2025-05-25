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


#graph_petrol_consumption_cts_sectors_2022()


################### candles applications petrol for CTS and industry 2022 ######################################################
path_output = "src/utils/thesis_outputs/petrol/applications"


#df1 = get_application_dissaggregation_factors(sector="industry", energy_carrier="petrol")

def graph_petrol_applications_cts_industry_2022():

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
    cts_path = "data/processed/pipeline/applications/disagg_applications_efficiency_factor/con_eff_2022_cts_petrol.csv"
    industry_path = "data/processed/pipeline/applications/disagg_applications_efficiency_factor/con_eff_2022_industry_petrol.csv"

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
    save_plot_with_datetime(plt, path_output, "petrol_applications_cts_industry_2022", dpi=300)
    

def graph_petrol_applications_cts_2022():

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
    cts_path = "data/processed/pipeline/applications/disagg_applications_efficiency_factor/con_eff_2022_cts_petrol.csv"
    cts_series = load_and_aggregate_cts(cts_path)

    # Plot
    plt.figure(figsize=(10, 6))
    cts_series.sort_values(ascending=False).plot(kind="bar", color="steelblue")
    plt.xlabel("Application")
    plt.ylabel("Petrol Consumption [TWh]")
    plt.xticks(ha='right')
    plt.tight_layout()
    save_plot_with_datetime(plt, path_output, "petrol_applications_cts_2022", dpi=300)

def graph_petrol_applications_industry_2022():
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
    industry_path = "data/processed/pipeline/applications/disagg_applications_efficiency_factor/con_eff_2022_industry_petrol.csv"
    industry_series = load_and_aggregate_industry(industry_path)

    # Plot
    plt.figure(figsize=(10, 6))
    industry_series.sort_values(ascending=False).plot(kind="bar", color="darkgreen")
    plt.xlabel("Application")
    plt.ylabel("Petrol Consumption [TWh]")
    plt.xticks(ha='right')
    plt.tight_layout()
    save_plot_with_datetime(plt, path_output, "petrol_applications_industry_2022", dpi=300)

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

#graph_petrol_applications_cts_industry_2022()
#graph_petrol_applications_cts_2022() used
#graph_petrol_applications_industry_2022() used
#graph_petrol_applications_industry_bysector_2022()


################### temporal for CTS and industry 2022 ######################################################
path_output = "src/utils/thesis_outputs/petrol/temporal"


#df1 = disaggregate_temporal(sector="cts", energy_carrier="petrol", year=2022, force_preprocessing=True)
#print("fertig1")

#df2 = disaggregate_temporal(sector="industry", energy_carrier="petrol", year=2022, force_preprocessing=True)
#print("fertig2")

#print("fertig")

def data_petrol_temporal_cts_2022():
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

    # === Step 4: Filter May and Resample to 2h ===
    df_may = df_combined.loc[df_combined.index.month == 5]
    df_may_2h = df_may.resample("2H").sum()

    # === Step 5: Save to file ===
    save_dataframe_with_datetime(df_may_2h, "petrol_consumption_cts_industry_may_2022_2h", path_output)

    # === Step 6: Plot ===
    plt.figure(figsize=(12, 6))
    plt.plot(df_may_2h.index, df_may_2h["industry"], label="Industry", color="tab:blue")
    plt.plot(df_may_2h.index, df_may_2h["cts"], label="CTS", color="tab:orange")

    plt.xlabel("Time (May 2022)")
    plt.ylabel("Petrol Consumption [MWh]")
    plt.title("Petrol Consumption by Sector – CTS vs. Industry (May 2022, 2h resolution)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    save_plot_with_datetime(plt, path_output, "petrol_consumption_cts_industry_may_2022_2h", dpi=300)


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

#data_petrol_temporal_cts_2022()
#data_petrol_temporal_cts_2022_daily()




################### temporal for CTS and industry 2022 ######################################################
path_output = "src/utils/thesis_outputs/petrol/heat"

# for year in [2020, 2025, 2030, 2035, 2040, 2045]:
#     for sector in ["cts", "industry"]:
#         df = disaggregate_temporal(sector=sector, energy_carrier="petrol", year=year, force_preprocessing=True, float_precision=8)


def sector_fuel_switch_fom_gas_petrol_cts():
    for year in [2020, 2025, 2030, 2035, 2040, 2045]:

        print(f"Processing year: {year}")


        def1 = sector_fuel_switch_fom_gas_petrol(year=year, sector="cts", energy_carrier="petrol", switch_to="power", force_preprocessing = True)

        save_dataframe_with_datetime(def1, f"sector_fuel_switch_fom_gas_petrol{year}", path_output)

def sector_fuel_switch_fom_gas_petrol_industry_power():
    for year in [2020, 2025, 2030, 2035, 2040, 2045]:

        print(f"Processing year: {year}")


        def1 = sector_fuel_switch_fom_gas_petrol(year=year, sector="industry", energy_carrier="petrol", switch_to="power", force_preprocessing = True)

        save_dataframe_with_datetime(def1, f"sector_fuel_switch_fom_gas_petrol{year}_industry_power", path_output)

def sector_fuel_switch_fom_gas_petrol_industry_hydrogen():
    for year in [2020, 2025, 2030, 2035, 2040, 2045]:

        print(f"Processing year: {year}")
        def1 = sector_fuel_switch_fom_gas_petrol(year=year, sector="industry", energy_carrier="petrol", switch_to="hydrogen", force_preprocessing = True)

        save_dataframe_with_datetime(def1, f"sector_fuel_switch_fom_gas_petrol{year}_industry_hydrogen", path_output)


def sector_fuel_switch_fom_gas_petrol_cts_power():
    years = [2020, 2025, 2030, 2035, 2040, 2045]
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


#sector_fuel_switch_fom_gas_petrol_industry_hydrogen()
#sector_fuel_switch_fom_gas_petrol_industry_power()
#sector_fuel_switch_fom_gas_petrol_cts_power()





def temporal_cts_elec_load_from_fuel_switch_petrol_power():
    for year in [2020, 2025, 2030, 2035, 2040, 2045]:

        print(f"Processing year: {year}")

        total_cts = pd.DataFrame()

        for state in federal_state_dict().values():
            def1 = temporal_cts_elec_load_from_fuel_switch_petrol(year=year, state=state, switch_to="power")
            def1 = def1.groupby(level='application', axis=1).sum()
            
            total_cts = pd.concat([total_cts, def1], axis=1)

        save_dataframe_with_datetime(total_cts, f"temporal_cts_elec_load_from_fuel_switch_petrol{year}_power", path_output)


temporal_cts_elec_load_from_fuel_switch_petrol_power()

"""this will retrun a df with:
index: daytime
column[0]: regional_ids
column[1]: industry_sectors
column[2]: applications
"""