# main_layer2.py

# === 1. IMPORTS ===

# general
from pathlib import Path
from typing import Union

# third party
import geopandas as gpd
import pandas as pd

# internal
from src.utils import (
    load_config,
    fetch_boundary, 
    buffer_boundary, 
    fetch_osmnx_points,
    deduplicate_points
)
from src.viz_layer2 import plot_poi


# === 2. MAIN LAYER 2 PIPELINE ===

def main_layer2():
    """
    Main pipeline for Layer 2 generation - fetches city boundary, buffers it, extracts POI points inside the city area, cleans them, and saves the results.
    Results are saved as: GeoJSON, HTML, Parquet.
    Logic:
        1. Load settings from YAML config file.
        2. Fetch the city boundary from OpenStreetMap using OSMNX.
        3. Buffer the boundary by a specified distance in meters.
        4. Fetches OSM POI points afferent to specific tags. 
        5. Cleans the POI points by removing duplicates based on a specified distance threshold and name similarity.
        5. Save the boundary and points as GeoJSON and Parquet files.
        6. Save a Folium HTML map visualising the points in layers for each category.
    """
    print("-> STARTING LAYER 2 PIPELINE...")

    # load settings.yaml
    config = load_config()
    
    # define and create output directories (extra safety)
    paths = config["paths"]
    processed_dir = Path(paths['processed'])
    viz_dir = Path(paths['viz'])
    maps_dir = Path(paths['maps'])
    dirs = [processed_dir, viz_dir, maps_dir]
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

    # fetch the city boundary from OSMNX
    city_name = config['project']['city_name']
    simple_boundary_gdf = fetch_boundary(city_name)

    # buffer the boundary by specified meters
    buffer_meters = config['grid']['buffer_dist_m']
    crs_metric = config['crs']['metric']

    buffered_boundary_gdf = buffer_boundary(
        simple_boundary_gdf, 
        buffer_meters,
        metric_crs = config['crs']['metric'],
        target_crs = config['crs']['global']
    )

    # fetch OSM POI settings from config
    sim_threshold = config["poi"]["deduplication"]["similarity_threshold"]
    clustering_dist_m = config["poi"]["deduplication"]["distance_m"]
    categories = config["poi"]["categories"]
    all_pois = []

    # extract points 
    print(f"-> Fetching OSM POI points within buffered boundary for {len(categories)} categories...")

    for category, tags in categories.items():
        # fetch points in this category
        print(f"-> Fetching category: {category}...")
        poi_raw_gdf = fetch_osmnx_points(buffered_boundary_gdf, tags)

        # check if any points were found
        if len(poi_raw_gdf) == 0:
            print(f"-> No points found for category: {category}. Skipping deduplication.")
            continue

        print(f"-> {len(poi_raw_gdf)} raw points found for category: {category}.") 

        print(f"-> Deduplicating category: {category}...")
        poi_gdf = deduplicate_points(
            poi_raw_gdf,
            distance_threshold_m = clustering_dist_m,
            metric_crs = crs_metric,
            similarity_threshold = sim_threshold,
            semantic_clustering=True
        )
        poi_gdf['category'] = category
        all_pois.append(poi_gdf)

    # combine all categories
    if not all_pois:
        print("-> No POI points found in any category. Exiting pipeline.")
        return
    
    print("-> Combining all categories into a single GeoDataFrame...")
    poi_gdf = pd.concat(all_pois, ignore_index=True)

    # save outputs
    print("-> Saving outputs...")

    # save Parquet files
    parquet_path = processed_dir / "l2_poi.parquet"
    poi_gdf.to_parquet(parquet_path, compression = "brotli")
    print(f"-> Saved POI points as Parquet at: {parquet_path}")

    # save GeoJSON
    geojson_path = viz_dir / "l2_poi.geojson"
    poi_gdf.to_file(geojson_path, driver="GeoJSON")
    print(f"-> Saved POI points as GeoJSON at: {geojson_path}")

    # save Folium HTML map
    map_path = maps_dir / "layer2_map.html"
    plot_poi(
        buffered_boundary_gdf,
        poi_gdf,
        save_path = map_path
    )
    print(f"-> Saved Folium map at: {map_path}")

    print("-> LAYER 2 PIPELINE COMPLETED.")


if __name__ == "__main__":
    main_layer2()