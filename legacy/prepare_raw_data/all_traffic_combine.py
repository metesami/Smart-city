## Combine all traffic data from different years and clean it and save it 
import pandas as pd
import numpy as np
from glob import glob
import os
import re
import json
from typing import Dict, List, Tuple

# === CONFIGURATION ===
traffic_data_root = '/content/drive/MyDrive/Test ontology_A142/Traffic'
a142_excel_path = '/content/drive/MyDrive/Test ontology_A142/A142_L5_20230901_complete.xlsx'
intersection_id = 'A142'

# === STEP 1: Extract sensor_ids from A142 Excel ===
a142_df = pd.read_excel(a142_excel_path)
sensor_ids = (
    a142_df['sensor_id']
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
    .tolist()
)

# === STEP 2: Build list of expected columns ===
base_columns = ['Anlage', 'Intervallbeginn (UTC)']
sensor_columns = []
for sensor_id in sensor_ids:
    sensor_columns.append(f"{sensor_id} (Belegungen/Intervall)")
    sensor_columns.append(f"{sensor_id} (Verweilzeit/Intervall) [ms]")
columns_to_keep = base_columns + sensor_columns

# === STEP 3: Load and filter all traffic CSV files ===
all_csv_paths = glob(os.path.join(traffic_data_root, '**', '*.csv'), recursive=True)
frames = []
for csv_path in all_csv_paths:
    try:
        df = pd.read_csv(csv_path, sep=';', encoding='utf-8')
        existing_cols = [col for col in columns_to_keep if col in df.columns]
        if not existing_cols:
            continue
        df = df[existing_cols].copy()

        # Keep only the target intersection
        if 'Anlage' in df.columns:
            df = df[df['Anlage'] == intersection_id]

        frames.append(df)
    except Exception as e:
        print(f" Error reading {csv_path}: {e}")


# === STEP 4: Combine all yearly CSVs ===
if not frames:
    raise RuntimeError("No valid traffic data found after filtering.")
traffic_all = pd.concat(frames, ignore_index=True)

# === STEP 6: Set datetime index for resampling ===
traffic_all['Intervallbeginn (UTC)'] = pd.to_datetime(
    traffic_all['Intervallbeginn (UTC)'], errors='coerce', utc=True
)
traffic_all = traffic_all.set_index('Intervallbeginn (UTC)').sort_index()

traffic_all = traffic_all.dropna(how='any')
traffic_all.isna().sum(axis=1).value_counts().sort_index()


out_path = f"{intersection_id}_traffic_1min.csv"
traffic_all.to_csv(out_path, index=True)