from pathlib import Path

import pandas as pd


BINS = {
    "NO2": {
        "bins": [0, 7.6, 10, 12.2, 14.5, 16.95, 20, 23.95, 30, 40, 60, float("inf")],
        "labels": [
            "NO2Bin_0p0_7p6_UGM3",
            "NO2Bin_7p6_10p0_UGM3",
            "NO2Bin_10p0_12p2_UGM3",
            "NO2Bin_12p2_14p5_UGM3",
            "NO2Bin_14p5_16p95_UGM3",
            "NO2Bin_16p95_20p0_UGM3",
            "NO2Bin_20p0_23p95_UGM3",
            "NO2Bin_23p95_30p0_UGM3",
            "NO2Bin_30p0_40p0_UGM3",
            "NO2Bin_40p0_60p0_UGM3",
            "NO2Bin_60p0_plus_UGM3",
        ],
        "output_col": "NO2_category",
    },
    "PM10": {
        "bins": [0, 6.60, 9.10, 11.85, 15, 16.95, 19.25, 22.65, 30, 50, 75, 100, float("inf")],
        "labels": [
            "PM10Bin_0p0_6p6_UGM3",
            "PM10Bin_6p6_9p1_UGM3",
            "PM10Bin_9p1_11p85_UGM3",
            "PM10Bin_11p85_15p0_UGM3",
            "PM10Bin_15p0_16p95_UGM3",
            "PM10Bin_16p95_19p25_UGM3",
            "PM10Bin_19p25_22p65_UGM3",
            "PM10Bin_22p65_30p0_UGM3",
            "PM10Bin_30p0_50p0_UGM3",
            "PM10Bin_50p0_75p0_UGM3",
            "PM10Bin_75p0_100p0_UGM3",
            "PM10Bin_100p0_plus_UGM3",
        ],
        "output_col": "PM10_category",
    },
    "PM2.5": {
        "bins": [0, 2.7, 3.55, 4.3, 5, 5.9, 6.95, 8.25, 10, 15, 25, 40, float("inf")],
        "labels": [
            "PM25Bin_0p0_2p7_UGM3",
            "PM25Bin_2p7_3p55_UGM3",
            "PM25Bin_3p55_4p3_UGM3",
            "PM25Bin_4p3_5p0_UGM3",
            "PM25Bin_5p0_5p9_UGM3",
            "PM25Bin_5p9_6p95_UGM3",
            "PM25Bin_6p95_8p25_UGM3",
            "PM25Bin_8p25_10p0_UGM3",
            "PM25Bin_10p0_15p0_UGM3",
            "PM25Bin_15p0_25p0_UGM3",
            "PM25Bin_25p0_40p0_UGM3",
            "PM25Bin_40p0_plus_UGM3",
        ],
        "output_col": "PM2.5_category",
    },
}


def load_pollution_table(input_path: str | Path) -> pd.DataFrame:
    input_path = Path(input_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Pollution input file not found: {input_path}")

    if input_path.suffix == ".csv":
        return pd.read_csv(input_path, parse_dates=["datetime"], low_memory=False)

    if input_path.suffix == ".parquet":
        return pd.read_parquet(input_path)

    raise ValueError(f"Unsupported input format: {input_path}")


def add_timestamp_seconds(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "datetime" not in df.columns:
        raise ValueError("Expected column 'datetime' not found.")

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
    df = df.dropna(subset=["datetime"])

    df["timestamp_seconds"] = (
        df["datetime"].astype("int64") // 10**9
    ).astype("int64")

    return df


def add_pollution_categories(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for source_col, spec in BINS.items():
        if source_col not in df.columns:
            continue

        df[spec["output_col"]] = pd.cut(
            pd.to_numeric(df[source_col], errors="coerce"),
            bins=spec["bins"],
            labels=spec["labels"],
            include_lowest=True,
        )

    return df


def categorize_pollution(
    input_path: str | Path,
    output_path: str | Path,
) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = load_pollution_table(input_path)
    df = add_timestamp_seconds(df)
    df = add_pollution_categories(df)

    df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    df.to_csv(output_path, index=False, encoding="utf-8")

    print("Pollution categorization finished.")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"Rows: {len(df)}")

    return output_path