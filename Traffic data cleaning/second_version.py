#!/usr/bin/env python3
# -*- coding: utf-8 -*-



import os
import re
import json
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd

CONFIG = {
    "timestamp_candidates": [
        "Intervallbeginn (UTC)",
        "Intervallbeginn",
        "timestamp",
        "time",
        "datetime"
    ],
    "minute_seconds": 60,
    # Physical ranges
    "avg_dwell_min_ms": 300,  # lower plausible average dwell per vehicle according to other articles in urban area
    "avg_dwell_max_ms": 10000,  # upper plausible average dwell per vehicle
    "dwell_abs_max_ms": 60000,
    # Capacity caps
    "global_cap_per_minute": 35, # global hard cap per minute (veh/min/lane)
    "cap_suspicious_lower": 30, # lower threshold for suspicious counts
    # Robust anomaly thresholds
    "spike_mad_multiplier": 5.0,
    "spike_window_minutes": 60,
    "zero_run_soft_minutes": 5,
    "stuck_off_minutes": 20,
    "stuck_on_minutes": 30,
     # Profile z-score (robust with IQR) thresholds
    "profile_group_minutes": 15,
    "profile_soft_z": 5.0,
    "profile_hard_z": 7.5,

    # Stuck on parameters
    "stuck_on_mean": 3,
    "stuck_on_std": 1e-6,
}
# Regex for sensor columns
COUNT_PAT = re.compile(r"^(?P<sid>[DV]\d+)\s*\(Belegungen/Intervall\)\s*$")
DWELL_PAT = re.compile(r"^(?P<sid>[DV]\d+)\s*\(Verweilzeit/Intervall\)\s*\[ms\]\s*$")

def find_timestamp_column(df: pd.DataFrame) -> str:
    """Return the timestamp column name for Darmstadt traffic data."""
    if "Intervallbeginn (UTC)" in df.columns:
        return "Intervallbeginn (UTC)"
    raise ValueError("Expected column 'Intervallbeginn (UTC)' not found in DataFrame.")

def extract_sensor_map(columns: List[str]) -> Dict[str, Dict[str, str]]:
    sensors: Dict[str, Dict[str, str]] = {}
    for c in columns:
        m1 = COUNT_PAT.match(c)
        if m1:
            sid = m1.group("sid")
            sensors.setdefault(sid, {})["count_col"] = c
            continue
        m2 = DWELL_PAT.match(c)
        if m2:
            sid = m2.group("sid")
            sensors.setdefault(sid, {})["dwell_col"] = c
    sensors = {k: v for k, v in sensors.items() if "count_col" in v and "dwell_col" in v}
    return sensors


def build_time_profiles(ts: pd.Series, values: pd.Series, profile_minutes: int = 15) -> pd.DataFrame:
    dt = ts.dt
    minutes = dt.hour * 60 + dt.minute
    bucket = (minutes // profile_minutes) * profile_minutes
    weekday = dt.weekday
    dfp = pd.DataFrame({"weekday": weekday, "bucket": bucket, "v": values})
    grp = dfp.groupby(["weekday", "bucket"])
    prof = grp["v"].agg(median=lambda s: np.nanmedian(s), q25=lambda s: np.nanpercentile(s, 25), q75=lambda s: np.nanpercentile(s, 75))
    prof["iqr"] = prof["q75"] - prof["q25"]
    prof = prof.drop(columns=["q25", "q75"])
    return prof

def compute_flags_for_sensor(df_sensor: pd.DataFrame) -> pd.DataFrame:
    out = df_sensor.copy().sort_values("timestamp").reset_index(drop=True)

    # تبدیل نوع‌ها برای سرعت و حافظه بهتر
    out["count_raw"] = pd.to_numeric(out["count_raw"], errors="coerce").astype("float32")
    out["dwell_raw"] = pd.to_numeric(out["dwell_raw"], errors="coerce").astype("float32")

    # S2 - phys & logic flags
    out["phys_flag"] = 0
    out["logic_flag"] = 0
    phys_invalid = (out["count_raw"] < 0) | (out["dwell_raw"] < 0) | (out["dwell_raw"] > CONFIG["dwell_abs_max_ms"])
    out.loc[phys_invalid, "phys_flag"] = 1

    cond = (out["count_raw"] == 0) & (out["dwell_raw"] > 0)
    out.loc[cond, "logic_flag"] = 1

    out["avg_dwell"] = np.where(out["count_raw"] > 0, out["dwell_raw"] / out["count_raw"].clip(lower=1.0), np.nan)
    avg_bad = ((out["count_raw"] > 0) & ((out["avg_dwell"] < CONFIG["avg_dwell_min_ms"]) | (out["avg_dwell"] > CONFIG["avg_dwell_max_ms"])))
    out.loc[avg_bad, "logic_flag"] = 1

    # S3 fixed cap
    gl_cap = CONFIG["global_cap_per_minute"]
    susp_low = CONFIG["cap_suspicious_lower"]
    out["cap_flag"] = (out["count_raw"] > gl_cap).astype("int8")
    out["cap_suspicious"] = ((out["count_raw"] > susp_low) & (out["count_raw"] <= gl_cap)).astype("int8")

    # S4 classic failures & spikes
    zero_mask = (out["count_raw"] == 0) & (out["dwell_raw"] == 0)

    # Soft (5 minutes)
    window_soft = CONFIG["zero_run_soft_minutes"]
    out["zero_run_soft"] = zero_mask.rolling(window=window_soft, min_periods=window_soft).sum().fillna(0).ge(window_soft).astype("int8")

    # Hard (20 minutes)
    window_off = CONFIG["stuck_off_minutes"]
    out["stuck_off"] = zero_mask.rolling(window=window_off, min_periods=window_off).sum().fillna(0).ge(window_off).astype("int8")

    # stuck_on using rolling std & mean
    window_on = CONFIG["stuck_on_minutes"]
    rolling_std = out["count_raw"].rolling(window=window_on, min_periods=window_on).std()
    rolling_mean = out["count_raw"].rolling(window=window_on, min_periods=window_on).mean()
    out["stuck_on"] = ((rolling_std.fillna(np.inf) < CONFIG["stuck_on_std"]) & (rolling_mean.fillna(0) > CONFIG["stuck_on_mean"])).astype("int8")

    # === Spike MAD: برداری بدون apply یا حلقه پایتون ===
    N = CONFIG["spike_window_minutes"]
    thr_mult = CONFIG["spike_mad_multiplier"]
    diffs = out["count_raw"].diff()

    # rolling median of diffs (برداری)
    rolling_med = diffs.rolling(window=N, min_periods=5).median()

    # MAD = median(|diff - rolling_med|)
    mad_series = (diffs - rolling_med).abs().rolling(window=N, min_periods=5).median()

    with np.errstate(invalid='ignore'):
        out["spike_flag"] = (diffs.abs() > thr_mult * mad_series).astype("int8").fillna(0).astype("int8")
    # =====================================================

    q75 = out["count_raw"].rolling(window=60, min_periods=20).quantile(0.75)
    q10 = out["count_raw"].rolling(window=60, min_periods=20).quantile(0.10)
    out["cliff_flag"] = ((out["count_raw"].shift(1) > q75) & (out["count_raw"] < q10)).astype("int8")

    # S6 - build time profiles (vectorized merge instead of Python loop)
    dt = out["timestamp"].dt
    minutes = dt.hour * 60 + dt.minute
    bucket = (minutes // CONFIG["profile_group_minutes"]) * CONFIG["profile_group_minutes"]
    weekday = dt.weekday

    prof_c = build_time_profiles(out["timestamp"], out["count_raw"], profile_minutes=CONFIG["profile_group_minutes"]).reset_index().rename(columns={"median": "c_med", "iqr": "c_iqr"})
    prof_d = build_time_profiles(out["timestamp"], out["dwell_raw"], profile_minutes=CONFIG["profile_group_minutes"]).reset_index().rename(columns={"median": "d_med", "iqr": "d_iqr"})

    out = out.assign(weekday=weekday.values, bucket=bucket.values)

    out = out.merge(prof_c[["weekday", "bucket", "c_med", "c_iqr"]], how="left", on=["weekday", "bucket"])
    out = out.merge(prof_d[["weekday", "bucket", "d_med", "d_iqr"]], how="left", on=["weekday", "bucket"])

    # robust z
    def robust_z_vec(x, med, iqr):
        denom = np.where(iqr > 0, iqr / 1.349, np.nan)
        with np.errstate(divide='ignore', invalid='ignore'):
            return (x - med) / denom

    zc = robust_z_vec(out["count_raw"].values.astype(float), out["c_med"].values.astype(float), out["c_iqr"].values.astype(float))
    zd = robust_z_vec(out["dwell_raw"].values.astype(float), out["d_med"].values.astype(float), out["d_iqr"].values.astype(float))

    out["profile_z_count"] = zc
    out["profile_z_dwell"] = zd
    out["profile_flag_soft"] = (((np.abs(zc) > CONFIG["profile_soft_z"]) | (np.abs(zd) > CONFIG["profile_soft_z"]))).astype("int8")
    out["profile_flag_hard"] = (((np.abs(zc) > CONFIG["profile_hard_z"]) | (np.abs(zd) > CONFIG["profile_hard_z"]))).astype("int8")

    # S7 combined flags
    out["soft_flag"] = (
        (out["logic_flag"] == 1) |
        (out["cap_suspicious"] == 1) |
        (out["zero_run_soft"] == 1) |
        (out["profile_flag_soft"] == 1)
    ).astype("int8")

    hard_flags = (
        (out["phys_flag"] == 1) |
        (out["cap_flag"] == 1) |
        (out["stuck_off"] == 1) |
        (out["stuck_on"] == 1) |
        (out["spike_flag"] == 1) |
        (out["cliff_flag"] == 1) |
        (out["profile_flag_hard"] == 1)
    )

    out["count_clean"] = out["count_raw"].astype(float).copy()
    out["dwell_clean"] = out["dwell_raw"].astype(float).copy()
    out.loc[hard_flags, ["count_clean", "dwell_clean"]] = np.nan

    cond_clean = (out["count_clean"] == 0) & (out["dwell_clean"] > 0)
    out.loc[cond_clean, "dwell_clean"] = 0.0

    # اگر لازم است ستون‌های کمکی را حذف کن (weekday, bucket, c_med...)
    out = out.drop(columns=[c for c in ["weekday", "bucket", "c_med", "c_iqr", "d_med", "d_iqr"] if c in out.columns])

    return out


def summarize_sensor(df: pd.DataFrame) -> Dict[str, float]:
    total = len(df)
    valid = np.sum(~df["count_clean"].isna() & ~df["dwell_clean"].isna())
    return {
        "minutes_total": int(total),
        "minutes_valid_both": int(valid),
        "valid_rate": float(valid / total) if total>0 else 0.0,
        "rate_phys": float(df["phys_flag"].mean()),
        "rate_cap": float(df["cap_flag"].mean()),
        "rate_cap_suspicious": float(df["cap_suspicious"].mean()),
        "rate_stuck_off": float(df["stuck_off"].mean()),
        "rate_stuck_on": float(df["stuck_on"].mean()),
        "rate_zero_run_soft": float(df["zero_run_soft"].mean()),
        "rate_spike": float(df["spike_flag"].mean()),
        "rate_cliff": float(df["cliff_flag"].mean()),
        "rate_profile_soft": float(df["profile_flag_soft"].mean()),
        "rate_profile_hard": float(df["profile_flag_hard"].mean()),
        "p95_count_raw": float(np.nanpercentile(df["count_raw"], 95)),
        "p95_count_clean": float(np.nanpercentile(df["count_clean"], 95)) if np.sum(~df["count_clean"].isna())>0 else np.nan,
    }

def load_input(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path)
    elif ext in [".csv"]:
        try:
            df = pd.read_csv(path)
        except Exception:
            df = pd.read_csv(path, sep=";")
    else:
        raise ValueError(f"Unsupported file extension: {ext}")
    return df

def wide_to_long(df: pd.DataFrame, ts_col: str, sensors: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    rows = []
    base_cols = [ts_col]
    for sid, cols in sensors.items():
        cnt_col = cols["count_col"]
        dwl_col = cols["dwell_col"]
        sub = df[base_cols + [cnt_col, dwl_col]].copy()
        sub.columns = [ts_col, "count_raw", "dwell_raw"]
        sub["sensor_id"] = sid
        rows.append(sub)
    long_df = pd.concat(rows, axis=0, ignore_index=True)
    long_df[ts_col] = pd.to_datetime(long_df[ts_col], errors="coerce", utc=True)
    long_df = long_df.rename(columns={ts_col: "timestamp"})
    long_df = long_df[~long_df["timestamp"].isna()].copy()
    long_df["count_raw"] = pd.to_numeric(long_df["count_raw"], errors="coerce").astype(float)
    long_df["dwell_raw"] = pd.to_numeric(long_df["dwell_raw"], errors="coerce").astype(float)
    return long_df

def process_file(input_path: str, outdir: str, intersection_id: str):
    os.makedirs(outdir, exist_ok=True)
    df = load_input(input_path)
    ts_col = find_timestamp_column(df)
    sensors = extract_sensor_map(list(df.columns))
    if not sensors:
        raise ValueError("No sensor pairs found. Ensure expected column names exist.")
    print(f"Found {len(sensors)} sensors.")

    df_long = wide_to_long(df, ts_col, sensors)

    out_frames = []
    summaries = []
    for sid in sensors.keys():
        sub = df_long[df_long["sensor_id"] == sid].copy()
        cleaned = compute_flags_for_sensor(sub)
        out_frames.append(cleaned)
        summ = summarize_sensor(cleaned)
        summ["sensor_id"] = sid
        summ["intersection_id"] = intersection_id
        summaries.append(summ)

    result = pd.concat(out_frames, axis=0, ignore_index=True)

    parquet_path = os.path.join(outdir, f"{intersection_id}_clean_pre_fusion.parquet")
    try:
        result.to_parquet(parquet_path, index=False)
    except Exception:
        parquet_path = None
        csv_fallback = os.path.join(outdir, f"{intersection_id}_clean_pre_fusion.csv.gz")
        result.to_csv(csv_fallback, index=False, compression="gzip")
        print(f"Parquet not available, saved CSV.gz instead: {csv_fallback}")

    summary_df = pd.DataFrame(summaries)
    summary_csv = os.path.join(outdir, f"{intersection_id}_sensor_summary.csv")
    summary_df.to_csv(summary_csv, index=False)

    with open(os.path.join(outdir, f"{intersection_id}_config.json"), "w") as f:
        json.dump(CONFIG, f, indent=2)

    print(f"Saved summary: {summary_csv}")


