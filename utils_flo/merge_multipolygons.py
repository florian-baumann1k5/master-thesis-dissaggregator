import pandas as pd
import geopandas
from shapely.wkt import loads
from shapely.ops import unary_union

def merge_polygons(df, id_ags_1, id_ags_2):
    """
    Merges the 'geom_as_text' MultiPolygon of row with id_ags_1 into the row with id_ags_2
    in a DataFrame based on their 'id_ags' values, effectively updating row with id_ags_2.

    Args:
        df (pd.DataFrame): The input DataFrame with 'id_ags' and 'geom_as_text' columns.
        id_ags_1 (int): The 'id_ags' value of the row to be merged into id_ags_2.
        id_ags_2 (int): The 'id_ags' value of the row that will contain the merged geometry.

    Returns:
        geopandas.GeoDataFrame: A new GeoDataFrame with the merged geometry in row id_ags_2.
                        Returns None if either id_ags_1 or id_ags_2 is not found.
    """
    # Filter the DataFrame for the two rows to be merged
    rows_to_merge = df[df['id_ags'].isin([id_ags_1, id_ags_2])]

    if len(rows_to_merge) != 2:
        print(f"Error: Could not find rows with id_ags {id_ags_1} and {id_ags_2}")
        return None

    # Convert the 'geom_as_text' column to geometry objects
    try:
        geometries = rows_to_merge['geom_as_text'].apply(loads)
    except Exception as e:
        print(f"Error: Could not convert 'geom_as_text' to geometry: {e}")
        return None

    # Use unary_union to merge the geometries
    merged_geometry = unary_union(geometries)

    # Update the geometry of the row with id_ags_2
    df.loc[df['id_ags'] == id_ags_2, 'geom_as_text'] = merged_geometry.wkt
    df.loc[df['id_ags'] == id_ags_2, 'geometry'] = merged_geometry

    # Remove the row with id_ags_1
    df = df[df['id_ags'] != id_ags_1].copy()

    # Convert the updated dataframe to a GeoDataFrame, using the existing 'geometry' column
    gdf = geopandas.GeoDataFrame(df, geometry='geometry')
    return gdf



def main():
    """
    Main function to demonstrate the usage of the merge_polygons function.
    """
    # Sample data (replace with your actual data loading)
    # data = {
    #     'idx': [0, 1, 2, 3, 4],
    #     'id_ags': [15090000, 15091000, 16056, 16063, 17000000],
    #     'gen': ['Stendal', 'Wittenberg', 'A', 'B', 'C'],
    #     'geom_as_text': [
    #         'MULTIPOLYGON(((690454.238801873 5802550.33474544, 690554.238801873 5802550.33474544, 690554.238801873 5802650.33474544, 690454.238801873 5802650.33474544, 690454.238801873 5802550.33474544)))',
    #         'MULTIPOLYGON(((758537.454094241 5729373.3059642, 758637.454094241 5729373.3059642, 758637.454094241 5729473.3059642, 758537.454094241 5729473.3059642, 758537.454094241 5729373.3059642)))',
    #         'MULTIPOLYGON(((700000 5800000, 701000 5800000, 701000 5801000, 700000 5801000, 700000 5800000)))',  # Example for 16056
    #         'MULTIPOLYGON(((702000 5802000, 703000 5802000, 703000 5803000, 702000 5803000, 702000 5802000)))',  # Example for 16063
    #         'MULTIPOLYGON(((800000 5900000, 801000 5900000, 801000 5901000, 800000 5901000, 800000 5900000)))'
    #     ],
    #     'fl_km2': [123.45, 67.89, 20.0, 25.0, 100.0]
    # }


    data = pd.read_csv("data/raw/regional/nuts3_shapefile.csv")
    
    df = pd.DataFrame(data)
    gdf = geopandas.GeoDataFrame(df, geometry=df['geom_as_text'].apply(loads))


    # Merge the polygons for id_ags 16056 into 16063
    merged_gdf = merge_polygons(gdf, 16056, 16063)

    if merged_gdf is not None:
        print(merged_gdf.head().to_markdown(index=False, numalign="left", stralign="left"))
        # Save the result to a CSV file
        merged_gdf.to_csv("nuts3_shapefile_2021.csv", index=False)
        print("Successfully merged the polygons, updated row with id_ags 16063, and saved the result to merged_polygons.csv.")
    else:
        print("Failed to merge polygons.")

if __name__ == "__main__":
    main()
