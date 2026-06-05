from pathlib import Path

import numpy as np
import pandas as pd


def load_clean_traffic(input_path: str | Path) -> pd.DataFrame:
    input_path = Path(input_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if input_path.suffix == ".parquet":
        return pd.read_parquet(input_path)

    if input_path.suffix == ".csv":
        return pd.read_csv(input_path, parse_dates=["timestamp"])

    if input_path.suffixes[-2:] == [".csv", ".gz"]:
        return pd.read_csv(input_path, parse_dates=["timestamp"], compression="gzip")

    raise ValueError(f"Unsupported input format: {input_path}")


def impute_zero_run_short(g: pd.DataFrame, limit: int = 5) -> pd.DataFrame:
    mask = g["missing_reason"] == "ZERO_RUN_SHORT"

    g.loc[mask, "count_imputed"] = (
        g["count_imputed"]
        .interpolate(method="linear", limit=limit, limit_direction="both")
    )

    g.loc[mask, "dwell_imputed"] = (
        g["dwell_imputed"]
        .interpolate(method="linear", limit=limit, limit_direction="both")
    )

    g.loc[mask & g["count_imputed"].notna(), "impute_method"] = "TEMPORAL_LINEAR"
    return g


def impute_spike(g: pd.DataFrame, window: int = 7, min_periods: int = 3) -> pd.DataFrame:
    mask = g["missing_reason"] == "SPIKE"

    rolling_count_median = (
        g["count_imputed"]
        .rolling(window=window, center=True, min_periods=min_periods)
        .median()
    )

    rolling_dwell_median = (
        g["dwell_imputed"]
        .rolling(window=window, center=True, min_periods=min_periods)
        .median()
    )

    g.loc[mask, "count_imputed"] = rolling_count_median.loc[mask]
    g.loc[mask, "dwell_imputed"] = rolling_dwell_median.loc[mask]
    g.loc[mask & g["count_imputed"].notna(), "impute_method"] = "ROLLING_MEDIAN"

    return g


def add_time_profile_columns(df: pd.DataFrame, profile_minutes: int = 15) -> pd.DataFrame:
    df = df.copy()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df["weekday"] = df["timestamp"].dt.weekday
    df["minute"] = df["timestamp"].dt.hour * 60 + df["timestamp"].dt.minute
    df["bucket"] = (df["minute"] // profile_minutes) * profile_minutes

    return df


def build_profile_table(df: pd.DataFrame) -> pd.DataFrame:
    profile = (
        df[df["missing_reason"] == "NONE"]
        .groupby(["sensor_id", "weekday", "bucket"])
        .agg(
            count_med=("count_clean", "median"),
            dwell_med=("dwell_clean", "median"),
        )
        .reset_index()
    )

    return profile


def impute_profile_soft(g: pd.DataFrame, profile: pd.DataFrame) -> pd.DataFrame:
    g = g.merge(
        profile,
        on=["sensor_id", "weekday", "bucket"],
        how="left",
    )

    mask = g["missing_reason"] == "PROFILE_SOFT"

    g.loc[mask, "count_imputed"] = g.loc[mask, "count_med"]
    g.loc[mask, "dwell_imputed"] = g.loc[mask, "dwell_med"]
    g.loc[mask & g["count_imputed"].notna(), "impute_method"] = "PROFILE_MEDIAN"

    return g.drop(columns=["count_med", "dwell_med"])


def run_layered_imputation(df: pd.DataFrame) -> pd.DataFrame:
    required_columns = {
        "timestamp",
        "sensor_id",
        "count_clean",
        "dwell_clean",
        "missing_reason",
    }

    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    df = df.copy()
    df = df.sort_values(["sensor_id", "timestamp"])

    df["count_imputed"] = df["count_clean"]
    df["dwell_imputed"] = df["dwell_clean"]
    df["impute_method"] = "NONE"

    df = add_time_profile_columns(df)
    profile = build_profile_table(df)

    out = []

    for _, g in df.groupby("sensor_id"):
        g = g.sort_values("timestamp").copy()
        g = impute_zero_run_short(g)
        g = impute_spike(g)
        g = impute_profile_soft(g, profile)
        out.append(g)

    df_imputed = pd.concat(out, ignore_index=True)

    helper_cols = ["weekday", "minute", "bucket"]
    df_imputed = df_imputed.drop(
        columns=[c for c in helper_cols if c in df_imputed.columns]
    )

    return df_imputed


def save_imputed_traffic(df: pd.DataFrame, output_path: str | Path) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.suffix == ".parquet":
        df.to_parquet(output_path, index=False)
    elif output_path.suffix == ".csv":
        df.to_csv(output_path, index=False)
    elif output_path.suffixes[-2:] == [".csv", ".gz"]:
        df.to_csv(output_path, index=False, compression="gzip")
    else:
        raise ValueError(f"Unsupported output format: {output_path}")

    return output_path


def run_traffic_imputation(
    input_path: str | Path,
    output_path: str | Path,
) -> Path:
    df = load_clean_traffic(input_path)
    df_imputed = run_layered_imputation(df)
    save_imputed_traffic(df_imputed, output_path)

    print("Traffic imputation finished.")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"Rows: {len(df_imputed)}")

    print("Imputation methods:")
    print(df_imputed["impute_method"].value_counts(dropna=False))

    return Path(output_path)