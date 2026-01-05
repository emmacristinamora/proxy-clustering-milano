# src/utils.py

# === 1.IMPORTS ===

# general
import yaml
from pathlib import Path
from typing import Dict, Any, Union
import os

# geospatial
import geopandas as gpd
import osmnx as ox


# === 2. CONFIGURATION UTILS ===

def load_config(config_path: Union[str, Path] = "config/settings.yaml") -> Dict[str, Any]:
    """
    Loads the project configuration from a YAML file.
    Args:
        config_path (Union[str, Path]): Path to the YAML file containing the settings.
    Returns:
        Dict[str, Any]: The configuration dictionary.
    """
    # convert string to Path object
    path_obj = Path(config_path)

    # check if file exists
    if not path_obj.exists():
        raise FileNotFoundError(f"!! Config file not found at: {config_path}")
    
    # load the YAML file
    with open(path_obj, "r") as f:
        config = yaml.safe_load(f)
    
    return config


# === 3. GEOSPATIAL UTILS ===

def fetch_boundary(city_name: str) -> gpd.GeoDataFrame:
    """
    Downloads the administrative boundary of a specified city (YAML file) from OpenStreetMap.
    Args:
        city_name (str): Name of the city to fetch the boundary for.
    Returns:
        gpd.GeoDataFrame: A df containing the city's boundary geometry in EPSG: 4326 coordinate system (Lat/Lon).
    """
    print(f"-> Fetching boundary for {city_name} from OSMNX...")

    # download the city's boundary in a robust way
    try:
        return ox.geocode_to_gdf(city_name) # OSM operates in EPSG: 4326 by default
    except Exception as e:
        raise ValueError(F"!! Couldn't fetch boundary for {city_name}. Error: {e}")
    
def buffer_boundary(
    gdf: gpd.GeoDataFrame,
    meters: int,
    metric_crs: str,
    target_crs: str = "EPSG:4326"
    ) -> gpd.GeoDataFrame:
    """
    Buffers the geospatial boundary of a city by a specified distance in meters.
    Logic:
        1. Reproject the gdf to the local metric system.
        2. Apply the buffer in meters.
        3. Reproject the buffered boundary back to the target CRS as a GeoDataFrame.
    Args:
        gdf (gdp.GeoDataFrame): GeoDataFrame containing the city's boundary geometry.
        meters (int): Distance in meters to buffer the boundary.
        metrics_crs (str): Local metric system (EPSG: "32632" for Italy)
        target_crs (str): Target coordinate reference system (default for H3 is "EPSG:4326").
    Returns:
        gpd.GeoDataFrame: A df containing the buffered city's boundary geometry in the target CRS.
    """
    print(f"-> Buffering boundary by {meters} meters...")

    # reproject to metric system
    gdf_metric = gdf.to_crs(metric_crs)

    # apply buffer
    gdf_metric["geometry"] = gdf_metric.geometry.buffer(meters)

    # reproject back to target CRS
    gdf_buffered = gdf_metric.to_crs(target_crs)

    return gdf_buffered