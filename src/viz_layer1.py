# src/viz_layer1.py

# === 1. IMPORTS ===

# general
from pathlib import Path
import os
from typing import Union

# third party
import geopandas as gpd
import folium


# === 2. VISUALISATION UTIL ===

def plot_transport(
    buffered_boundary: gpd.GeoDataFrame,
    transport_gdf: gpd.GeoDataFrame,
    save_path: Union[str, Path] = "outputs/maps/layer1_map.html"
    ) -> None:
    """
    Plots the buffered boundary and OSM transport points on a Folium map and saves it as an HTML file.
    Args:
        buffered_boundary (gpd.GeoDataFrame): GeoDataFrame containing the buffered boundary geometry.
        transport_gdf (gpd.GeoDataFrame): GeoDataFrame containing the cleaned transport points.
        save_path (Union[str, Path]): Path to save the HTML map file.
    Returns:
        None
    Logic:
        1. Metro -> red circles
        2. Train -> blue circles
        3. Tram -> green cicles
        4. Boundary -> black outline
    """
    print("-> Plotting boundary and transport points on Folium map...")

    # create a Folium map centered around the buffered boundary
    centroid = buffered_boundary.to_crs("EPSG:3857").geometry.centroid.to_crs("EPSG:4326").iloc[0]
    m = folium.Map(location=[centroid.y, centroid.x], zoom_start=11, tiles="CartoDB positron")

    # add the metros (red circles)
    fg_metro = folium.FeatureGroup(name="Metro Stations")
    for _, row in transport_gdf[transport_gdf['type'] == 'metro'].iterrows():
        folium.CircleMarker(
            location = [row.geometry.y, row.geometry.x],
            radius = 5, color = 'red', fill = True, fill_opacity = 0.6, weight = 0.5,
            popup = f"Metro: {row['name']}"
        ).add_to(fg_metro)
   
    fg_metro.add_to(m)

    # add the trains (blue circles)
    fg_train = folium.FeatureGroup(name="Train Stations")
    for _, row in transport_gdf[transport_gdf['type'] == 'train'].iterrows():
        folium.CircleMarker(
            location = [row.geometry.y, row.geometry.x],
            radius = 5, color = 'blue', fill = True, fill_opacity = 0.6, weight = 0.5,
            popup = f"Train: {row['name']}"
        ).add_to(fg_train)
   
    fg_train.add_to(m)

    # add the trams (green circles)
    fg_tram = folium.FeatureGroup(name="Tram Stations")
    for _, row in transport_gdf[transport_gdf['type'] == 'tram'].iterrows():
        folium.CircleMarker(
            location = [row.geometry.y, row.geometry.x],
            radius = 3, color = 'green', fill = True, fill_opacity = 0.4, weight = 0.5,
            popup = f"Tram: {row['name']}"
        ).add_to(fg_tram)
   
    fg_tram.add_to(m)

    # add the buffered boundary (red, transparent)
    folium.GeoJson(
        buffered_boundary,
        name = "Buffered Boundary",
        style_function = lambda x: {
            'color': 'black',
            'weight': 0.8,
            'fillOpacity': 0.0
        }
    ).add_to(m)

    # add layer control
    folium.LayerControl().add_to(m)

    # save the map as an HTML file
    m.save(save_path)
    print(f"-> Map saved at: {save_path}")

    return None


def plot_cleaned_comparison(
    original_gdf: gpd.GeoDataFrame,
    cleaned_gdf: gpd.GeoDataFrame,
    buffered_boundary: gpd.GeoDataFrame,
    layer_name: str,
    save_path: Union[str, Path] = "outputs/maps/cleaned_transport_comparison_map.html"   
) -> None:
    """
    Plots the original and cleaned transport points on a Folium map for comparison and saves it as an HTML file.
    Args:
        original_gdf (gpd.GeoDataFrame): GeoDataFrame containing the original transport points.
        cleaned_gdf (gpd.GeoDataFrame): GeoDataFrame containing the cleaned transport points.
        buffered_boundary (gpd.GeoDataFrame): GeoDataFrame containing the buffered boundary geometry.
        layer_name (str): Name of the transport layer.
        save_path (Union[str, Path]): Path to save the HTML map file.
    Returns:
        None
    Logic:
        1. Original points -> red circles
        2. Cleaned points -> green circles
        3. Boundary -> black outline
    """
    print("-> Plotting original vs cleaned transport points on Folium map...")
    
    # create a Folium map centered around the buffered boundary
    centroid = buffered_boundary.to_crs("EPSG:3857").geometry.centroid.to_crs("EPSG:4326").iloc[0]
    m = folium.Map(location=[centroid.y, centroid.x], zoom_start=11, tiles="CartoDB positron")

    # add the original points (red circles)
    fg_original = folium.FeatureGroup(name=f"Original {layer_name} (Red)")
    for _, row in original_gdf.iterrows():
        folium.CircleMarker(
            location = [row.geometry.y, row.geometry.x],
            radius = 3, color = 'red', fill = True, fill_opacity = 0.2, weight = 0.5,
            popup = f"Original: {row['name']}"
        ).add_to(fg_original)
   
    fg_original.add_to(m)

    # add the cleaned points (green circles)
    fg_cleaned = folium.FeatureGroup(name=f"Cleaned {layer_name} (Green)")
    for _, row in cleaned_gdf.iterrows():
        folium.CircleMarker(
            location = [row.geometry.y, row.geometry.x],
            radius = 4, color = 'green', fill = True, fill_opacity = 0.2, weight = 0.5,
            popup = f"Cleaned: {row['name']}"
        ).add_to(fg_cleaned)
   
    fg_cleaned.add_to(m)

    # add the buffered boundary (black outline)
    folium.GeoJson(
        buffered_boundary,
        name = "Buffered Boundary",
        style_function = lambda x: {
            'color': 'black',
            'weight': 0.5,
            'fillOpacity': 0.0
        }
    ).add_to(m)

    # add layer control
    folium.LayerControl().add_to(m)

    # save the map as an HTML file
    m.save(save_path)
    print(f"-> Map saved at: {save_path}")

    return None
