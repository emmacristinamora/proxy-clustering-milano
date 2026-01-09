# src/fetch_insideairbnb.py


# === 1. IMPORTS ===

import requests
import gzip
import shutil
import pandas as pd
import h3
from typing import Union
from pathlib import Path
from src.utils import load_config, assign_h3


# === 2. FUNCTION TO FETCH AND PROCESS INSIDE AIRBNB DATA ===

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
    Logic:
        1. Load the CSV data from the specified file path.
        2. Filter listings based on specified criteria from settings.yaml.
        3. Assign H3 indices to each listing based on latitude and longitude.
        4. Compute per person per night price for each H3 cell.
        5. Return the processed DataFrame.
    """
    print(f"-> Processing Inside Airbnb data from {file_path}...")

    # load data and settings
    config = load_config()
    paths = config['paths']
    filters = config['data_sources']['inside_airbnb']['filters']
    filename = config['data_sources']['inside_airbnb']['filename']

    # paths
    input_path = Path(paths['raw']) / file_path
    output_path = Path(paths['processed']) / "insideairbnb_h3.csv"

    # load data
    print(f"-> Loading {filename} data from {input_path}...")
    cols = ['id', 'latitude', 'longitude', 'price', 'accommodates', 'room_type', 'minimum_nights', 'number_of_reviews', 'last_review']
    df = pd.read_csv(input_path, usecols=cols)
    original_len = len(df)
    
    # clean price column
    if df['price'].dtype == 'object':
        df['price'] = df['price'].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    # drop rows with no price or capacity
    df = df.dropna(subset=['price', 'accommodates'])
    df = df[df['accommodates'] > 0]

    # create price_pp column
    df['price_pp'] = df['price'] / df['accommodates']

    # apply filters
    print(f"-> Applying filters to {filename} data...")

    # entire home
    df = df[df['room_type'] == filters['room_type']]

    # minimum nights (so we only have short term rentals)
    df = df[df['minimum_nights'] <= filters['max_minimum_nights']]

    # number of reviews
    df = df[df['number_of_reviews'] >= filters['min_reviews']]

    # must have recent reviews
    if filters.get('must_have_recent_review'):
        one_year_ago = pd.Timestamp.now() - pd.DateOffset(months=12)
        df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce')
        df = df[df['last_review'] >= one_year_ago]

    # remove outliers based on price_pp
    low_q = df['price_pp'].quantile(filters['price_pp_quantile_low'])
    high_q = df['price_pp'].quantile(filters['price_pp_quantile_high'])
    df = df[df['price_pp'].between(low_q, high_q)]

    print(f"-> Filtered {original_len - len(df)} listings; {len(df)} remain.")
    print(f"-> Valid price/perperson range: {low_q:.2f} - {high_q:.2f}")

    # assign H3 indices
    df = assign_h3(df, 'latitude', 'longitude', h3_resolution)

    # compute per hexagon statistics
    h3_stats = df.groupby('h3_index').agg(
        listings_count=('id', 'count'),
        avg_price_pp=('price_pp', 'mean'),
        avg_capacity=('accommodates', 'mean'),
        avg_price_original=('price', 'mean')
    ).reset_index()

    # save processed data
    h3_stats.to_csv(output_path, index=False)
    h3_stats.to_parquet(output_path.with_suffix('.parquet'), index=False)
    print(f"-> Processed data saved to {output_path} and {output_path.with_suffix('.parquet')}.")
    print("Processing complete.")
    return h3_stats

if __name__ == "__main__":
    process_str_data(
        file_path = "insideairbnb_milano_sept2025.csv",
        h3_resolution = 10
    )