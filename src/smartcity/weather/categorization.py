from pathlib import Path

import pandas as pd


BINS = {
    "temperature": {
        "bins": [-12.7, 0.0, 10.0, 20.0, 30.0, float("inf")],
        "labels": [
            "TempBin_minus12p7_0p0_DEGC",
            "TempBin_0p0_10p0_DEGC",
            "TempBin_10p0_20p0_DEGC",
            "TempBin_20p0_30p0_DEGC",
            "TempBin_30p0_plus_DEGC",
        ],
        "output_col": "temperature_category",
    },
    "precipitation": {
        "bins": [0.0, 0.1, 1.0, 5.0, 10.0, float("inf")],
        "labels": [
            "RainBin_0p0_0p1_MM",
            "RainBin_0p1_1p0_MM",
            "RainBin_1p0_5p0_MM",
            "RainBin_5p0_10p0_MM",
            "RainBin_10p0_plus_MM",
        ],
        "output_col": "precipitation_category",
    },
    "humidity": {
        "bins": [0, 50.0, 70.0, 85.0, 95.0, 100.0],
        "labels": [
            "HumidityBin_0p0_50p0_PCT",
            "HumidityBin_50p0_70p0_PCT",
            "HumidityBin_70p0_85p0_PCT",
            "HumidityBin_85p0_95p0_PCT",
            "HumidityBin_95p0_100p0_PCT",
        ],
        "output_col": "humidity_category",
    },
    "pressure": {
        "bins": [900, 980.0, 995.0, 1005.0, 1015.0, float("inf")],
        "labels": [
            "PressureBin_900p0_980p0_HPA",
            "PressureBin_980p0_995p0_HPA",
            "PressureBin_995p0_1005p0_HPA",
            "PressureBin_1005p0_1015p0_HPA",
            "PressureBin_1015p0_plus_HPA",
        ],
        "output_col": "pressure_category",
    },
    "wind_speed": {
        "bins": [0, 1.0, 3.0, 5.0, 10.0, float("inf")],
        "labels": [
            "WindBin_0p0_1p0_MS",
            "WindBin_1p0_3p0_MS",
            "WindBin_3p0_5p0_MS",
            "WindBin_5p0_10p0_MS",
            "WindBin_10p0_plus_MS",
        ],
        "output_col": "wind_speed_category",
    },
}


WIND_DIRECTION_BINS = [0, 45, 90, 135, 180, 225, 270, 315, 360]
WIND_DIRECTION_LABELS = [
    "WindDir_N",
    "WindDir_NE",
    "WindDir_E",
    "WindDir_SE",
    "WindDir_S",
    "WindDir_SW",
    "WindDir_W",
    "WindDir_NW",
]


def load_weather_table(input_path: str | Path) -> pd.DataFrame:
    input_path = Path(input_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Weather input file not found: {input_path}")

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


def add_numeric_weather_categories(df: pd.DataFrame) -> pd.DataFrame:
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


def add_wind_direction_category(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "wind_direction" not in df.columns:
        return df

    deg = pd.to_numeric(df["wind_direction"], errors="coerce") % 360
    deg_rot = (deg + 22.5) % 360

    df["wind_direction_category"] = pd.cut(
        deg_rot,
        bins=WIND_DIRECTION_BINS,
        labels=WIND_DIRECTION_LABELS,
        include_lowest=True,
    )

    return df


def categorize_weather(
    input_path: str | Path,
    output_path: str | Path,
) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = load_weather_table(input_path)

    df = add_timestamp_seconds(df)
    df = add_numeric_weather_categories(df)
    df = add_wind_direction_category(df)

    # Keep datetime in consistent ISO format for downstream ABox
    df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    df.to_csv(output_path, index=False, encoding="utf-8")

    print("Weather categorization finished.")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"Rows: {len(df)}")

    return output_path