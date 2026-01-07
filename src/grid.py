# src/grid.py


# === 1. IMPORTS ===

# general
from typing import List, Set, Union

# geospatial 
import geopandas as gpd
import h3
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry


# === 2. GENERATE H3 HEXAGON GRID ===

def generate_h3_grid(
    area: Union[Polygon, MultiPolygon, BaseGeometry],
    resolution: int
    ) -> gpd.GeoDataFrame:
    """
    Generates an H3 hexagon grid covering a boundary of a given area.
    Args:
        area (gpd.GeoDataFrame): GeoDataFrame containing the boundary geometry.
        resolution (int): H3 resolution level (0-15).
    Returns:
        gpd.GeoDataFrame: A df containing the H3 hexagon grid covering the area.
    Logic:
        1. Extract the geometry of the area. It might be a Polygon or MultiPolygon. 
        2. Coordinates must be swapped from (Lon, Lat) of Shapely/Geopandas to (Lat, Lon) of H3. 
        We must make sure we don't miss any interior gap.
        3. Fill the are with H3 hexagons at the specified resolution.
        4. Convert H3 hexagons to Shapely polygons and create a GeoDataFrame. Convert (Lat, Lon) back to (Lon, Lat).
    """
    print(f"-> Generating H3 grid at resolution {resolution}...")

    # extract the geometry of the area (convert MultiPolygon to list of Polygons if needed)
    if isinstance(area, MultiPolygon):
        polys = list(area.geoms)
    else:
        polys = [area]
    
    # use a Set to avoid duplicates if polygons overlap slightly
    hex_ids: Set[str] = set()

    # coordinate swap 
    for poly in polys:
        # extract exterior gpd coords and swap (Lon, Lat) to (Lan, Lot) as required by H3
        exterior_coords = [(lat, lon) for lon, lat in poly.exterior.coords]
        # extract interior holes (eg. a lake in the poly)
        interior_coords = [
            [(lat, lon) for lon, lat in interior.coords]
            for interior in poly.interiors
        ]

        # fill the polygon with H3 hexagons robustly
        try:
            # create a LatLngPoly object as H3 requires
            poly_obj = h3.LatLngPoly(exterior_coords, *interior_coords)
            # fill the polygon with hexagons
            filled_hexes = h3.polygon_to_cells(poly_obj, resolution)
            # add to the Set
            hex_ids.update(filled_hexes)
        except Exception as e:
            print(f"!! Warning: Couldn't fill a polygon part: {e}")
    
    print(f"-> Generated {len(hex_ids)} hexagons covering the area.")

    # reconstruct geometry and convert back to (Lon, Lat)
    hex_data = []
    for hex_id in hex_ids:
        hex_boundary_latlon = h3.cell_to_boundary(hex_id) # returns list of (Lat, Lon)
        # swap back to (Lon, Lat)
        hex_boundary_lonlat = [(lon, lat) for lat, lon in hex_boundary_latlon]
        # create Shapely polygon
        hex_polygon = Polygon(hex_boundary_lonlat)
        hex_data.append(
            {
                "h3_index": hex_id,
                "geometry": hex_polygon
            }
        )

    # create GeoDataFrame
    hex_gdf = gpd.GeoDataFrame(hex_data, geometry="geometry", crs="EPSG:4326")

    return hex_gdf