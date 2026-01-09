# src/features.py

# === 1. IMPORTS ===

# general
import os
from pathlib import Path

# third party
import pandas as pd
import geopandas as gpd
from sklearn.feature_extraction.text import TfidfTransformer

# internal
from src.utils import load_config, assign_h3


# === 2. LAYER CONVERSION UTIL ===

def normalize_transport_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes the transport column to match the POI data. Safety measure in case the layers differ in structure (technically they shouldn't but better be safe than sorry).
    Args:
        df (pd.DataFrame): DataFrame containing the transport layer.
    Returns:
        pd.DataFrame: checked DataFrame which can be compared safely with Layer 2.
    """
    df = df.copy()

    if 'sub_category' not in df.columns:
        for col in ['station', 'railway', 'highway', 'public_transport']:
            if col in df.columns:
                df['sub_category'] = df[col]
                break
        
        # fallback if still missing
        if 'sub_category' not in df.columns:
            df['sub_category'] = 'transport_node'
        
        # normalize string
        df['sub_category'] = df['sub_category'].astype(str).str.lower().str.strip()

    return df


# === 3. FEATURES PIPELINE ===

def build_features():
    """
    Builds the features for our model. Loads Layer 1 (transport) and Layer 2 (POis), assigns to them H3 indices, and applies TF-IDF normalization.
    """
    print("-> Starting feature matrix generation...")

    # load data
    config = load_config()
    paths = config['paths']
    res = config['grid']['resolution']
    processed_dir = Path(paths['processed'])
    viz_dir = Path(paths['viz'])
    out_path = processed_dir / "l3_features_tfidf.parquet"

    # load layers
    print("-> Loading Layer 1 - Transport and Layer 2 - POIs...")
    if (processed_dir / "l2_poi.parquet").exists():
        poi_gdf = gpd.read_parquet(processed_dir / "l2_poi.parquet") 
    else:
        poi_gdf = gpd.read_file(processed_dir / "l2_poi.geojson") 

    if (processed_dir / "l1_transport.parquet").exists():
        transport_gdf = gpd.read_parquet(processed_dir / "l1_transport.parquet") 
    else:
        transport_gdf = gpd.read_file(processed_dir / "l1_transport.geojson") 

    # extract coordinates
    poi_gdf = poi_gdf.to_crs("EPSG:4326") 
    poi_gdf['latitude'] = poi_gdf.geometry.y
    poi_gdf['longitude'] = poi_gdf.geometry.x

    transport_gdf = transport_gdf.to_crs("EPSG:4326")
    transport_gdf['latitude'] = transport_gdf.geometry.y
    transport_gdf['longitude'] = transport_gdf.geometry.x

    # map to h3
    print(f"-> Mapping points to h3 resolution {res}...")
    poi_df = assign_h3(poi_gdf, 'latitude', 'longitude', res)
    poi_df = poi_df[['h3_index', 'sub_category']].copy()
    transport_df = assign_h3(transport_gdf,'latitude', 'longitude', res)
    transport_df = normalize_transport_columns(transport_df)
    transport_df = transport_df[['h3_index', 'sub_category']].copy()

    # merge dataframes
    print("-> Creating H3-points matrix...")
    master_df = pd.concat([poi_df, transport_df])

    # pivot table such that rows = h3, cols = POI amenities, values = counts
    raw_counts_matrix = pd.crosstab(master_df['h3_index'], master_df['sub_category'])
    print(f"-> Matrix Shape: {raw_counts_matrix.shape} (hexagons x features)")

    # apply TF-IDF normalization
    print("Applying TD-IDF normalization (scarcity weighting)")
    tfidf = TfidfTransformer(smooth_idf=True, norm='l2')
    tfidf_matrix = pd.DataFrame(
        tfidf.fit_transform(raw_counts_matrix).toarray(),
        index = raw_counts_matrix.index,
        columns = raw_counts_matrix.columns
    )

    # save
    tfidf_matrix.to_parquet(out_path)
    raw_counts_matrix.to_parquet(processed_dir / "l3_features_raw.parquet")

    # check
    print("\nTop 5 most important features across the city:")
    print(tfidf_matrix.max().sort_values(ascending=False).head(5))


if __name__ == '__main__':
    build_features()
