# src/utils.py

# === 1.IMPORTS ===

# general
import yaml
from pathlib import Path
from typing import Dict, Any, Union, List
import os
import re
import numpy as np

# geospatial
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point, Polygon, MultiPolygon

# semantic
from difflib import SequenceMatcher

# ml
from sklearn.cluster import DBSCAN


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

def names_are_similar(name_a: str, name_b: str, threshold: float) -> bool:
    """
    Checks if two names are similar enough to be the same entity.
    """ 
    # exact match
    if name_a in name_b or name_b in name_a:
        return True
    
    # fuzzy match (Levenshtein distance)
    return SequenceMatcher(None, name_a, name_b).ratio() >= threshold


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

def fetch_osmnx_points(
    boundary: Union[gpd.GeoDataFrame, Polygon, MultiPolygon],
    tags: Dict[str, Union[str, List[str]]]
) -> gpd.GeoDataFrame:
    """
    Fetches points with specified OSM tags within a given boundary using OSMNX.
    Args:
        boundary (gpd.GeoDataFrame): GeoDataFrame containing the boundary geometry.
        tags (Dict[str, Union[str, List[str]]]): Dictionary of OSM tags to filter points.
    Returns:
        gpd.GeoDataFrame: A df containing the fetched points within the boundary (name, geometry, sub_category).
    """
    print(f"-> Fetching OSMNX points with tags {tags}...")

    # define the search geometry
    if isinstance(boundary, gpd.GeoDataFrame):
        search_geometry = boundary.union_all()  # combine all geometries into one
    else:
        search_geometry = boundary

    # extract in a robust way
    try:
        # fetch features
        points_gdf = ox.features_from_polygon(
            search_geometry,
            tags=tags
        )

        # return only points (OSMNX may return polygons for buldings for eg so we compute centroids)
        points_gdf = points_gdf.to_crs("EPSG:3857")  # project to metric system for accurate centroid calculation
        points_gdf['geometry'] = points_gdf.geometry.centroid
        points_gdf = points_gdf.to_crs("EPSG:4326")  # back to Lat/Lon

        # column to store sub-category types
        points_gdf['sub_category'] = "unknown"
        for key in tags.keys():
            if key in points_gdf.columns:
                points_gdf['sub_category'] = points_gdf['sub_category'].where(
                    points_gdf[key].isna(),
                    points_gdf[key]
                )

        # filter columns to keep only "name", "geometry", and "sub_category"
        cols_to_keep = ['name', 'geometry', 'sub_category']
        points_gdf = points_gdf[cols_to_keep].copy()

        if 'name' not in points_gdf.columns:
            points_gdf['name'] = "Unnamed POI"  # add empty name column if not present
        else:
            points_gdf = points_gdf.dropna(subset=['name'])  # drop points without a name

        return points_gdf
    
    except Exception as e:
        print(f"!! No data found for {tags}: {e}")
        return gpd.GeoDataFrame(columns=['name', 'geometry', 'sub_category'], geometry='geometry', crs='EPSG:4326')

def deduplicate_points(
    points_gdf: gpd.GeoDataFrame,
    distance_threshold_m: float,
    metric_crs: str,
    similarity_threshold: float = 0.6,
    semantic_clustering: bool = True
) -> gpd.GeoDataFrame:
    """
    Deduplicates points in a GeoDataFrame using ENTITY RESOLUTION. Spatial clustering is done with DBSCAN. Semantic clustering is done after the spatial one if flag is TRUE.
    Args:
        points_gdf (gpd.GeoDataFrame): GeoDataFrame containing the points to deduplicate.
        distance_threshold_m (float): Distance threshold in meters for spatial clustering.
        metric_crs (str): Local metric system (EPSG: "32632" for Italy).    
        semantic_clustering (bool): Whether to perform semantic clustering after spatial clustering.
        similarity_threshold (float): Similarity threshold for semantic clustering (between 0 and 1).
    Returns:
        gpd.GeoDataFrame: A df containing the deduplicated points.

    """
    print(f"-> Deduplicating {len(points_gdf)} points using spatial clustering...")
    print(f"-> Semantic clustering is set to: {semantic_clustering}")

    # corner case
    if len(points_gdf) <= 1:
        print("-> Not enough points to deduplicate. Returning original GeoDataFrame.")
        return points_gdf
    
    # project to metric system
    points_metric = points_gdf.to_crs(metric_crs).copy()

    # spatial clustering with DBSCAN
    coords = np.array(list(zip(points_metric.geometry.x, points_metric.geometry.y)))
    spatial_clustering = DBSCAN(
        eps = distance_threshold_m,
        min_samples = 1
    ).fit(coords)
    points_metric['spatial_cluster'] = spatial_clustering.labels_

    cleaned_rows = []

    print(f"-> Performing semantic clustering...")

    # process clusters (and semantic clustering if flag is TRUE)
    for cluster_id, group in points_metric.groupby('spatial_cluster'):

        # if cluster has only one point or semantic clustering is disabled, keep it as is
        if len(group) == 1 or not semantic_clustering:
            centroid = group.geometry.union_all().centroid

            # pick a representative name (first non-empty)
            name = group['name'].mode()[0] if not group['name'].mode().empty else group.iloc[0]['name']
            sub_category = group['sub_category'].mode()[0] if 'sub_category' in group and not group['sub_category'].mode().empty else "unknown"

            cleaned_rows.append({
                'name': name,
                'geometry': centroid,
                'sub_category': sub_category,
                'node_count': len(group)
            })
            continue

        # semantic clustering TRUE

        items = group.to_dict('records')
        processed_indices = set()

        for i in range(len(items)):
            if i in processed_indices:
                continue

            # start a new subgroup with the first item
            current_subgroup = [items[i]]
            processed_indices.add(i)

            # compare with all other items in this spatial cluster
            for j in range(i + 1, len(items)):
                if j in processed_indices:
                    continue

                # compute similarity (simple ratio of common words in names)
                name_a = str(items[i].get('name', '')).lower()
                name_b = str(items[j].get('name', '')).lower()

                if names_are_similar(name_a, name_b, threshold=similarity_threshold):
                    current_subgroup.append(items[j])
                    processed_indices.add(j)

            # compute centroid of the subgroup
            geoms = [d['geometry'] for d in current_subgroup]
            names = [d['name'] for d in current_subgroup]
            sub_categories = [d.get('sub_category', 'unknown') for d in current_subgroup]

            centroid = gpd.GeoSeries(geoms).union_all().centroid


            # pick representative name & sub_category (most common)
            best_name = min(names, key=len)
            best_sub_category = max(set(sub_categories), key=sub_categories.count)

            cleaned_rows.append({
                'name': best_name,
                'geometry': centroid,
                'sub_category': best_sub_category,
                'node_count': len(current_subgroup)
            })

    print(f"-> Reduced to {len(cleaned_rows)} deduplicated points after spatial and semantic clustering.")
    
    # reconstruct cleaned gdf
    return gpd.GeoDataFrame(cleaned_rows, crs=metric_crs).to_crs("EPSG:4326")