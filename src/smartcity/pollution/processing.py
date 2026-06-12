from pathlib import Path
import re

import numpy as np
import pandas as pd


NUM_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?")


def normalize_freq(freq: str) -> str:
    freq = freq.strip().lower()
    if freq.endswith("min") or freq.endswith("h"):
        return freq
    raise ValueError("Unsupported freq. Use examples like: 10min, 15min, 30min, 1h")


def extract_first_numeric_token_to_float(value):
    if pd.isna(value):
        return np.nan

    match = NUM_RE.search(str(value).strip())
    if not match:
        return np.nan

    token = match.group(0).replace(",", ".")

    try:
        return float(token)
    except Exception:
        return np.nan


def robust_load_pollution(file_path: str | Path) -> pd.DataFrame:
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Pollution file not found: {file_path}")

    df = pd.read_csv(
        file_path,
        sep=";",
        dtype=str,
        encoding="utf-8",
        low_memory=False,
    )

    if "Datum" not in df.columns or "Zeit" not in df.columns:
        raise ValueError(
            f"Expected 'Datum' and 'Zeit' columns in {file_path}. "
            f"Found columns: {df.columns.tolist()}"
        )

    original_time = df["Zeit"].astype(str).fillna("")

    datetime_text = (
        df["Datum"].astype(str).fillna("")
        + " "
        + original_time.str.replace("24:00", "00:00", regex=False)
    )

    datetime_parsed = pd.to_datetime(
        datetime_text,
        dayfirst=True,
        format="%d.%m.%Y %H:%M",
        errors="coerce",
    )

    mask_24 = original_time == "24:00"

    if mask_24.any():
        dates_only = pd.to_datetime(
            df.loc[mask_24, "Datum"],
            dayfirst=True,
            format="%d.%m.%Y",
            errors="coerce",
        )
        datetime_parsed.loc[mask_24] = dates_only + pd.Timedelta(days=1)

    datetime_utc = (
        datetime_parsed
        .dt.tz_localize("CET", ambiguous="NaT", nonexistent="shift_forward")
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )

    out = pd.DataFrame({"datetime": datetime_utc})

    suspect_columns = [
        column
        for column in df.columns
        if any(
            token in column.lower()
            for token in ["no2", "pm10", "pm2", "stickstoff", "pm2,5"]
        )
    ]

    for column in suspect_columns:
        out[column] = df[column].apply(extract_first_numeric_token_to_float)

    rename_map = {
        "Stickstoffdioxid (NO₂)[µg/m³]": "NO2",
        "PM10[µg/m³]": "PM10",
        "PM2,5[µg/m³]": "PM2.5",
    }

    rename_map = {old: new for old, new in rename_map.items() if old in out.columns}
    out = out.rename(columns=rename_map)

    return out


def resample_pollution_station(
    df: pd.DataFrame,
    station_id: str,
    freq: str = "10min",
    numeric_columns: tuple[str, ...] = ("NO2", "PM10", "PM2.5"),
    fill_limit: int | None = 2,
) -> pd.DataFrame:
    freq = normalize_freq(freq)

    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"]).set_index("datetime").sort_index()

    if df.index.duplicated().any():
        df = df.groupby(level=0).mean(numeric_only=True)

    keep_columns = [column for column in numeric_columns if column in df.columns]

    for column in keep_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    resampled = df[keep_columns].resample(freq).asfreq()

    if fill_limit is not None and fill_limit > 0:
        resampled = resampled.ffill(limit=fill_limit)

    resampled["StationID"] = station_id

    return resampled.reset_index()


def combine_pollution(
    station_files: dict[str, str | Path],
    output_path: str | Path,
    freq: str = "10min",
    fill_limit: int | None = 2,
) -> Path:
    freq = normalize_freq(freq)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    frames = []

    for station_id, file_path in station_files.items():
        raw = robust_load_pollution(file_path)
        station_df = resample_pollution_station(
            raw,
            station_id=station_id,
            freq=freq,
            fill_limit=fill_limit,
        )
        frames.append(station_df)

    if not frames:
        raise RuntimeError("No pollution station files were processed.")

    result = pd.concat(frames, ignore_index=True)

    result["datetime"] = pd.to_datetime(result["datetime"], errors="coerce", utc=True)
    result = result.dropna(subset=["datetime"])

    result["timestamp_seconds"] = (
        result["datetime"].astype("int64") // 10**9
    ).astype("int64")

    result["freq"] = freq

    result["datetime"] = result["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    pollutants = [column for column in ["NO2", "PM10", "PM2.5"] if column in result.columns]

    result = result[
        ["datetime", "timestamp_seconds", "StationID", "freq"] + pollutants
    ].sort_values(["StationID", "timestamp_seconds"])

    result.to_csv(output_path, index=False, encoding="utf-8")

    print("Pollution processing finished.")
    print(f"Stations: {list(station_files.keys())}")
    print(f"Frequency: {freq}")
    print(f"Rows: {len(result)}")
    print(f"Output: {output_path}")

    return output_path