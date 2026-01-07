# main_layer1.py

# === 1. IMPORTS ===

# general
from pathlib import Path

# thrid party
import pandas as pd
import geopandas as gpd

# internal
from src.utils import (
    load_config, 
    names_are_similar,
    fetch_boundary, 
    buffer_boundary, 
    fetch_osmnx_points, 
    deduplicate_points
)
from src.viz_layer1 import plot_transport, plot_cleaned_comparison


# === 2. MAIN LAYER 1 PIPELINE ===

def main_layer1():
    """
    Main pipeline for Layer 1 generation - fetches city boundary, buffers it, extracts transport points inside the city area, cleans them, and saves the results.
    Results are saved as: GeoJSON, HTML, Parquet.
    Logic:
        1. Load settings from YAML config file.
        2. Fetch the city boundary from OpenStreetMap using OSMNX.
        3. Buffer the boundary by a specified distance in meters.
        4. Fetches OSM transport points afferent to specific tags. Trains should not include points already tagged as metro.
        5. Cleans the transport points by removing duplicates based on a specified distance threshold and name similarity.
        5. Save the boundary and points as GeoJSON and Parquet files.
        6. Save a Folium HTML map visualising the cleaning comparison and the final set of points.
    """
    print("-> STARTING LAYER 1 PIPELINE...")

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

    # fetch OSM transport settings from config
    sim_threshold = config["transport"]["deduplication"]["similarity_threshold"]
    metro_dist_m = config["transport"]["deduplication"]["metro_dist_m"]
    train_dist_m = config["transport"]["deduplication"]["train_dist_m"]
    tram_dist_m = config["transport"]["deduplication"]["tram_dist_m"]
    metro_tags = config["transport"]["tags"]["metro"]
    train_tags = config["transport"]["tags"]["train"]
    tram_tags = config["transport"]["tags"]["tram"]

    # extract points 
    print(f"-> Fetching OSM transport points within buffered boundary...")
    metro_raw_gdf = fetch_osmnx_points(buffered_boundary_gdf, metro_tags)
    train_raw_gdf = fetch_osmnx_points(buffered_boundary_gdf, train_tags)
    tram_raw_gdf = fetch_osmnx_points(buffered_boundary_gdf, tram_tags)

    # make sure trains do not include metro points
    print("-> Removing metro points from train dataset...")
    initial_train_count = len(train_raw_gdf)
    train_raw_gdf = train_raw_gdf[~train_raw_gdf.index.isin(metro_raw_gdf.index)]
    print(f"-> Removed {initial_train_count - len(train_raw_gdf)} metro points from train dataset.")

    # deduplicate points
    cleaned_datasets = {}

    # deduplicate metro 
    print("-> Deduplicating metro points...")

    metro_gdf = deduplicate_points(
        metro_raw_gdf, 
        distance_threshold_m = metro_dist_m, 
        metric_crs = crs_metric, 
        similarity_threshold = sim_threshold, 
        semantic_clustering=True
    )

    cleaned_datasets['metro'] = metro_gdf
    plot_cleaned_comparison(metro_raw_gdf, metro_gdf, buffered_boundary_gdf, "Metro", save_path = maps_dir / "cleaned_metro_comparison_map.html")

    # deduplicate tram
    print("-> Deduplicating tram points...")

    tram_gdf = deduplicate_points(
        tram_raw_gdf, 
        distance_threshold_m = tram_dist_m, 
        metric_crs = crs_metric, 
        similarity_threshold = sim_threshold, 
        semantic_clustering=True
    )

    cleaned_datasets['tram'] = tram_gdf
    plot_cleaned_comparison(tram_raw_gdf, tram_gdf, buffered_boundary_gdf, "Tram", save_path = maps_dir / "cleaned_tram_comparison_map.html")

    # deduplicate train
    print("-> Deduplicating train points...")

    train_gdf = deduplicate_points(
        train_raw_gdf, 
        distance_threshold_m = train_dist_m, 
        metric_crs = crs_metric, 
        similarity_threshold = sim_threshold, 
        semantic_clustering=True
    )

    cleaned_datasets['train'] = train_gdf
    plot_cleaned_comparison(train_raw_gdf, train_gdf, buffered_boundary_gdf, "Train", save_path = maps_dir / "cleaned_train_comparison_map.html")

    # save outputs
    print("-> Saving outputs...")
    metro_gdf['type'] = 'metro'
    train_gdf['type'] = 'train'
    tram_gdf['type'] = 'tram'
    transport_gdf = pd.concat([metro_gdf, train_gdf, tram_gdf], ignore_index=True)

    # save Parquet files
    parquet_path = processed_dir / "l1_transport.parquet"
    transport_gdf.to_parquet(parquet_path, compression = "brotli")
    print(f"-> Saved transport points as Parquet at: {parquet_path}")

    # save GeoJSON
    geojson_path = viz_dir / "l1_transport.geojson"
    transport_gdf.to_file(geojson_path, driver="GeoJSON")
    print(f"-> Saved transport points as GeoJSON at: {geojson_path}")

    # save Folium HTML map
    map_path = maps_dir / "layer1_map.html"
    plot_transport(
        buffered_boundary_gdf,
        transport_gdf,
        save_path = map_path
    )
    print(f"-> Saved Folium map at: {map_path}")

    print("-> LAYER 1 PIPELINE COMPLETED.")


if __name__ == "__main__":
    main_layer1()