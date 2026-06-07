from pathlib import Path
from glob import glob
from typing import List

import pandas as pd


def load_sensor_ids(metadata_file: str | Path, sensor_column: str = "sensor_id") -> List[str]:
    metadata_file = Path(metadata_file)

    if not metadata_file.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_file}")

    if metadata_file.suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(metadata_file)
    elif metadata_file.suffix == ".csv":
        df = pd.read_csv(metadata_file)
    else:
        raise ValueError(f"Unsupported metadata file format: {metadata_file.suffix}")

    if sensor_column not in df.columns:
        raise ValueError(f"Column '{sensor_column}' not found in metadata file.")

    sensor_ids = (
        df[sensor_column]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    if not sensor_ids:
        raise ValueError("No sensor IDs found in metadata file.")

    return sensor_ids


def build_expected_traffic_columns(sensor_ids: List[str]) -> List[str]:
    base_columns = ["Anlage", "Intervallbeginn (UTC)"]

    sensor_columns = []
    for sensor_id in sensor_ids:
        sensor_columns.append(f"{sensor_id} (Belegungen/Intervall)")
        sensor_columns.append(f"{sensor_id} (Verweilzeit/Intervall) [ms]")

    return base_columns + sensor_columns


def read_traffic_csv(csv_path: str | Path) -> pd.DataFrame:
    csv_path = Path(csv_path)

    read_kwargs = {
        "sep": ";",
        "encoding": "utf-8",
        "engine": "python",
        "on_bad_lines": "warn",
    }

    try:
        return pd.read_csv(csv_path, **read_kwargs)
    except UnicodeDecodeError:
        read_kwargs["encoding"] = "latin1"
        return pd.read_csv(csv_path, **read_kwargs)

def combine_traffic_files(
    traffic_root: str | Path,
    metadata_file: str | Path,
    intersection_id: str,
    output_path: str | Path,
    sensor_column: str = "sensor_id",
    drop_missing_rows: bool = True,
) -> Path:
    traffic_root = Path(traffic_root)
    output_path = Path(output_path)

    if not traffic_root.exists():
        raise FileNotFoundError(f"Traffic root not found: {traffic_root}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    sensor_ids = load_sensor_ids(metadata_file, sensor_column=sensor_column)
    columns_to_keep = build_expected_traffic_columns(sensor_ids)

    csv_paths = sorted(glob(str(traffic_root / "**" / "*.csv"), recursive=True))

    if not csv_paths:
        raise RuntimeError(f"No CSV files found under: {traffic_root}")

    frames = []

    for csv_path in csv_paths:
        try:
            df = read_traffic_csv(csv_path)

            existing_cols = [col for col in columns_to_keep if col in df.columns]

            if not existing_cols:
                continue

            df = df[existing_cols].copy()

            if "Anlage" in df.columns:
                df = df[df["Anlage"].astype(str) == str(intersection_id)]

            if not df.empty:
                frames.append(df)

        except Exception as error:
            print(f"Error reading {csv_path}: {error}")

    if not frames:
        raise RuntimeError(
            f"No valid traffic data found for intersection {intersection_id}."
        )

    traffic_all = pd.concat(frames, ignore_index=True)

    if "Intervallbeginn (UTC)" not in traffic_all.columns:
        raise ValueError("Expected timestamp column 'Intervallbeginn (UTC)' not found.")

    traffic_all["Intervallbeginn (UTC)"] = pd.to_datetime(
        traffic_all["Intervallbeginn (UTC)"],
        errors="coerce",
        utc=True,
        dayfirst=True,
    )

    traffic_all = traffic_all.dropna(subset=["Intervallbeginn (UTC)"])
    traffic_all = traffic_all.sort_values("Intervallbeginn (UTC)")

    if drop_missing_rows:
        traffic_all = traffic_all.dropna(how="any")

    traffic_all.to_csv(output_path, index=False)

    print("Traffic combine finished.")
    print(f"Intersection: {intersection_id}")
    print(f"Sensors found in metadata: {len(sensor_ids)}")
    print(f"CSV files scanned: {len(csv_paths)}")
    print(f"Output rows: {len(traffic_all)}")
    print(f"Output: {output_path}")

    return output_path