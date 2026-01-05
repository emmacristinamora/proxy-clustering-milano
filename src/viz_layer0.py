# src/viz_layer0.py

# === 1. IMPORTS ===

# general
from pathlib import Path
import os
from typing import Union

# third party
import geopandas as gpd
import folium


# === 2. VISUALISATION UTIL ===

def plot_boundary_and_grid(
    buffered_boundary: gpd.GeoDataFrame,
    grid: gpd.GeoDataFrame,
    save_path: Union[str, Path] = "outputs/maps/layer0_map.html"
    ) -> None:
    """
    Plots the buffered boundary and H3 grid on a Folium map and saves it as an HTML file.
    Args:
        buffered_boundary (gpd.GeoDataFrame): GeoDataFrame containing the buffered boundary geometry.
        grid (gpd.GeoDataFrame): GeoDataFrame containing the H3 hexagon grid.
        save_path (Union[str, Path]): Path to save the HTML map file.
    Returns:
        None
    """
    print("-> Plotting boundary and H3 grid on Folium map...")

    # create a Folium map centered around the buffered boundary
    centroid = buffered_boundary.to_crs("EPSG:3857").geometry.centroid.to_crs("EPSG:4326").iloc[0]
    m = folium.Map(location=[centroid.y, centroid.x], zoom_start=11, tiles="CartoDB positron")

    # add the grid (light blue, transparent)
    folium.GeoJson(
        grid,
        name = "H3 Grid",
        style_function = lambda x: {
            'color': 'blue',
            'weight': 0.5,
            'fillOpacity': 0.1
        },
        tooltip=folium.GeoJsonTooltip(fields=['h3_index'])
    ).add_to(m)

    # add the buffered boundary (red, transparent)
    folium.GeoJson(
        buffered_boundary,
        name = "Buffered Boundary",
        style_function = lambda x: {
            'color': 'red',
            'weight': 1,
            'fillOpacity': 0.0
        }
    ).add_to(m)

    # add layer control
    folium.LayerControl().add_to(m)

    # save the map as an HTML file
    m.save(save_path)
    print(f"-> Map saved at: {save_path}")

    return None