# main_layer0.py

# === 1. IMPORTS ===

# general
from pathlib import Path

# third party
import geopandas as gpd

# internal
from src.utils import load_config, fetch_boundary, buffer_boundary
from src.grid import generate_h3_grid
from src.viz_layer0 import plot_boundary_and_grid


# === 2. MAIN LAYER 0 PIPELINE ===

def main_layer0():
    """
    Main pipeline for Layer 0 generation - fetches city boundary, buffers it, generates H3 grid, and saves the results.
    Results are saved as: GeoJSON, HTML, Parquet.
    Logic:
        1. Load settings from YAML config file.
        2. Fetch the city boundary from OpenStreetMap using OSMNX.
        3. Buffer the boundary by a specified distance in meters.
        4. Generate an H3 hexagon grid covering the buffered area at a specified respolution.
        5. Save the boundary and grid as GeoJSON and Parquet files.
        6. Save a Folium HTML map visualising the boundary and grid.
    """
    print("-> STARTING LAYER O PIPELINE...")

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
    buffered_boundary_gdf = buffer_boundary(
        simple_boundary_gdf, 
        buffer_meters,
        metric_crs = config['crs']['metric'],
        target_crs = config['crs']['global']
    )

    # grid generation
    res = config['grid']['resolution']
    h3_grid_gdf = generate_h3_grid(
        buffered_boundary_gdf.geometry.values[0],
        res
    )

    # save Parquet
    parquet_path = processed_dir / "l0_grid.parquet"
    h3_grid_gdf.to_parquet(parquet_path, compression = "brotli")
    print(f"-> Saved H3 grid as Parquet at: {parquet_path}")

    # save GeoJSON
    geojson_path = viz_dir / "l0_grid.geojson"
    h3_grid_gdf.to_file(geojson_path, driver="GeoJSON")
    print(f"-> Saved H3 grid as GeoJSON at: {geojson_path}")

    # save Folium HTML map
    map_path = maps_dir / "layer0_map.html"
    plot_boundary_and_grid(
        buffered_boundary_gdf,
        h3_grid_gdf,
        save_path = map_path
    )
    print(f"-> Saved Folium map at: {map_path}")

    print("-> LAYER 0 PIPELINE COMPLETED.")


if __name__ == "__main__":
    main_layer0()