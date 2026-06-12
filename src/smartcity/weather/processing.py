from pathlib import Path

import numpy as np
import pandas as pd


def normalize_freq(freq: str) -> str:
    freq = freq.strip().lower()

    if freq.endswith("min") or freq.endswith("h"):
        return freq

    raise ValueError("Unsupported freq. Use examples like: 10min, 15min, 30min, 1h")


def parse_mess_datum(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    sample = s.dropna().iloc[0] if s.dropna().shape[0] else ""
    fmt = "%Y%m%d%H%M" if len(sample) >= 12 else "%Y%m%d%H"
    return pd.to_datetime(s, format=fmt, errors="coerce", utc=True)


def clean_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()

    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
            df[column] = df[column].replace(-999, np.nan)

    return df


def load_station_id(metadata_path: str | Path) -> str:
    metadata = pd.read_csv(metadata_path, encoding="utf-8", low_memory=False)

    if "StationID" not in metadata.columns or metadata.empty:
        raise ValueError("Weather metadata must contain a non-empty StationID column.")

    return str(metadata.iloc[0]["StationID"]).strip()


def load_temperature_humidity(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", decimal=",", encoding="utf-8", low_memory=False)
    df["datetime"] = parse_mess_datum(df["MESS_DATUM"])
    df = df.dropna(subset=["datetime"]).set_index("datetime").sort_index()

    keep = [c for c in ["TT_10", "RF_10"] if c in df.columns]
    df = clean_numeric(df[keep].copy(), keep)

    return df.rename(columns={"TT_10": "temperature", "RF_10": "humidity"})


def load_precipitation(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", decimal=",", encoding="utf-8", low_memory=False)
    df["datetime"] = parse_mess_datum(df["MESS_DATUM"])
    df = df.dropna(subset=["datetime"]).set_index("datetime").sort_index()

    keep = [c for c in ["RWS_10", "RWS_IND_10"] if c in df.columns]
    df = clean_numeric(df[keep].copy(), keep)

    if "RWS_10" in df.columns:
        df = df.rename(columns={"RWS_10": "precipitation"})

    if "RWS_IND_10" in df.columns:
        df["rain_flag"] = df["RWS_IND_10"]
        df = df.drop(columns=["RWS_IND_10"])

    return df


def load_pressure(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", decimal=",", encoding="utf-8", low_memory=False)
    df["datetime"] = parse_mess_datum(df["MESS_DATUM"])
    df = df.dropna(subset=["datetime"]).set_index("datetime").sort_index()

    if "  P0" in df.columns:
        df = df[["  P0"]].rename(columns={"  P0": "pressure"})
    elif "P0" in df.columns:
        df = df[["P0"]].rename(columns={"P0": "pressure"})
    elif "PP_10" in df.columns:
        df = df[["PP_10"]].rename(columns={"PP_10": "pressure"})
    else:
        df = df.iloc[:, :1].rename(columns={df.columns[0]: "pressure"})

    return clean_numeric(df, ["pressure"])


def align_pressure_to_freq(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    freq = normalize_freq(freq)
    df = df.copy().sort_index()

    last_time = df.index.max()
    if pd.notna(last_time):
        df.loc[last_time + pd.to_timedelta(freq)] = df.iloc[-1].values

    return df.resample(freq).asfreq().ffill()


def load_wind(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", decimal=",", encoding="utf-8", low_memory=False)
    df["datetime"] = parse_mess_datum(df["MESS_DATUM"])
    df = df.dropna(subset=["datetime"]).set_index("datetime").sort_index()

    keep = [c for c in ["FF_10", "DD_10"] if c in df.columns]
    df = clean_numeric(df[keep].copy(), keep)

    return df.rename(columns={"FF_10": "wind_speed", "DD_10": "wind_direction"})


def aggregate_weather(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    freq = normalize_freq(freq)

    agg_rules = {}

    if "temperature" in df.columns:
        agg_rules["temperature"] = "mean"
    if "humidity" in df.columns:
        agg_rules["humidity"] = "mean"
    if "pressure" in df.columns:
        agg_rules["pressure"] = "mean"
    if "precipitation" in df.columns:
        agg_rules["precipitation"] = "sum"
    if "wind_speed" in df.columns:
        agg_rules["wind_speed"] = "mean"
    if "wind_direction" in df.columns:
        agg_rules["wind_direction"] = "mean"
    if "rain_flag" in df.columns:
        agg_rules["rain_flag"] = "max"

    return df.resample(freq).agg(agg_rules)


def combine_weather(
    metadata_path: str | Path,
    temperature_path: str | Path,
    precipitation_path: str | Path,
    pressure_path: str | Path,
    wind_path: str | Path,
    output_path: str | Path,
    freq: str = "10min",
) -> Path:
    freq = normalize_freq(freq)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    station_id = load_station_id(metadata_path)

    df_temp = load_temperature_humidity(temperature_path)
    df_prec = load_precipitation(precipitation_path)
    df_press = align_pressure_to_freq(load_pressure(pressure_path), freq=freq)
    df_wind = load_wind(wind_path)

    merged = pd.concat(
        [df_temp, df_prec, df_press, df_wind],
        axis=1,
        join="outer",
    ).sort_index()

    aggregated = aggregate_weather(merged, freq=freq)

    result = aggregated.reset_index().rename(columns={"index": "datetime"})
    result["datetime"] = pd.to_datetime(result["datetime"], utc=True, errors="coerce")
    result = result.dropna(subset=["datetime"])

    result["timestamp_seconds"] = (
        result["datetime"].astype("int64") // 10**9
    ).astype("int64")

    result["StationID"] = station_id
    result["freq"] = freq
    result["datetime"] = result["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    base_columns = ["datetime", "timestamp_seconds", "StationID", "freq"]
    value_columns = [
        c
        for c in [
            "temperature",
            "humidity",
            "pressure",
            "precipitation",
            "wind_speed",
            "wind_direction",
            "rain_flag",
        ]
        if c in result.columns
    ]

    result = result[base_columns + value_columns].sort_values(
        ["StationID", "timestamp_seconds"]
    )

    result.to_csv(output_path, index=False, encoding="utf-8")

    print("Weather processing finished.")
    print(f"StationID: {station_id}")
    print(f"Frequency: {freq}")
    print(f"Rows: {len(result)}")
    print(f"Output: {output_path}")

    return output_path