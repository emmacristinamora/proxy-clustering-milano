# src/viz_layer2.py

# === 1. IMPORTS ===

# general
from pathlib import Path
from typing import Union, Dict

# third party
import geopandas as gpd
import folium


# === 2. VISUALISATION UTIL ===

CATEGORY_COLORS = {
    'food_nightlife': '#ff9900', # orange
    'culture_leisure': '#9b59b6', # purple
    'public_services': '#3498db', # blue
    'retail_shopping': '#e74c3c', # red
    'business_economy': '#34495e', # dark blue
    'unknown': '#95a5a6'  # grey (fallback)
}

def plot_poi(
    buffered_boundary: gpd.GeoDataFrame,
    poi_gdf: gpd.GeoDataFrame,
    save_path: Union[str, Path] = "outputs/maps/layer2_map.html"
    ) -> None:
    """
    Plots the OSM POI points on a Folium map and saves it as an HTML file.
    Args:
        buffered_boundary (gpd.GeoDataFrame): GeoDataFrame containing the buffered boundary geometry.
        poi_gdf (gpd.GeoDataFrame): GeoDataFrame containing the cleaned POI points.
        save_path (Union[str, Path]): Path to save the HTML map file.
    Returns:
        None
    Logic:
        1. Group by 'category' and plot each category with different colors/markers. Each category gets its own layer and color.
        2. Subcategories are shown as text in the popup.
        3. Boundary -> black outline
    """
    print("-> Plotting POI points on Folium map...")

    # create a Folium map centered around the buffered boundary
    centroid = buffered_boundary.to_crs("EPSG:3857").geometry.centroid.to_crs("EPSG:4326").iloc[0]
    m = folium.Map(location=[centroid.y, centroid.x], zoom_start=11, tiles="CartoDB positron", control_scale=True)

    # add the boundary (black outline)
    folium.GeoJson(
        buffered_boundary,
        name = "Boundary",
        style_function = lambda x: {
            'color': 'black',
            'weight': 0.8,
            'fill': False,
            'dashArray': '5, 5' # dashed line
        }
    ).add_to(m)

    # add the POIs by category
    categories = poi_gdf['category'].unique()
    for category in categories:
        color = CATEGORY_COLORS.get(category, CATEGORY_COLORS['unknown'])

        # format category name for layer
        layer_name = f"{category.replace('_', ' ').title()}"
        fg = folium.FeatureGroup(name = f"<span style='color: {color}'>.</span> {layer_name}")

        # filter data for this category
        subset = poi_gdf[poi_gdf['category'] == category]

        # add points to this layer
        for _, row in subset.iterrows():
            # add Tooltip for seeing the sub_category
            tooltip_text = (
                f"<b>{row['name']}</b><br>"
                f"<i>{row['sub_category']}</i>"
            )

            folium.CircleMarker(
                location = [row.geometry.y, row.geometry.x],
                radius = 2.5,
                color = color,
                fill = True,
                fill_opacity = 0.6,
                weight = 0.0,
                tooltip = tooltip_text
            ).add_to(fg)
        
        # add the layer to the map
        fg.add_to(m)

    # add layer control
    folium.LayerControl(collapsed = False).add_to(m)

    # save the map as an HTML file
    m.save(save_path)
    print(f"-> Map saved at: {save_path}")

    return None
