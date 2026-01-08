# src/fetch_insideairbnb.py


# === 1. IMPORTS ===

import requests
import gzip
import shutil
import pandas as pd
import h3
from typing import Union
from pathlib import Path
from src.utils import load_config


# === 2. FUNCTION TO FETCH AND PROCESS INSIDE AIRBNB DATA ===

def _assign_h3(
        df: pd.DataFrame,
        lat_col: str,
        lon_col: str,
        res: int
) -> pd.DataFrame:
    """
    Assigns H3 hexagonal grid indices to each listing based on latitude and longitude. Handles different versions of the h3 library for safety.
    Args:
        df (pd.DataFrame): DataFrame containing listing data with latitude and longitude columns.
        lat_col (str): Name of the latitude column.
        lon_col (str): Name of the longitude column.
        res (int): H3 resolution level.
    Returns:
        pd.DataFrame: DataFrame with an additional 'h3_index' column.
    """
    try:
        df['h3_index'] = df.applu(lambda x: h3.latlng_to_cell(x[lat_col], x[lon_col], res), axis = 1)
    except AttributeError:
        df['h3_index'] = df.apply(lambda row: h3.geo_to_h3(row[lat_col], row[lon_col], res), axis=1)
    return df

def process_str_data(
        file_path: Union[str, Path],
        h3_resolution: int
) -> pd.DataFrame:
    """
    Processes Inside Airbnb data for Milan by extracting listings, cleaning, and assigning H3 indices.
    Args:
        file_path (Union[str, Path]): Path to the Inside Airbnb CSV data file.
        h3_resolution (int): H3 resolution level for indexing.

    Returns:
        pd.DataFrame: Processed DataFrame with H3 indices.
    """
    print(f"-> Processing Inside Airbnb data from {file_path}...")

    # load data and settings
    config = load_config()
    paths = config['paths']
    filters = config['data_sources']['inside_airbnb']['filters']