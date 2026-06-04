# part for convert .parquet to .csv
import pandas as pd

df = pd.read_parquet("/run/determined/workdir/Cleaned_data/A142_clean_pre_fusion.parquet")
df.to_csv("/run/determined/workdir/Cleaned_data/A142_clean_pre_fusion_full.csv.gz",
          index=False, compression="gzip")




# import for imputation
import pandas as pd
import numpy as np

df_main = pd.read_csv(
    "/run/determined/workdir/Cleaned_data/A142_clean_pre_fusion_full.csv.gz",
    parse_dates=["timestamp"]
)
df = df.sort_values(["sensor_id", "timestamp"])


# create imputed columns
df["count_imputed"] = df["count_clean"]
df["dwell_imputed"] = df["dwell_clean"]
df["impute_method"] = "NONE"


# Define imputation functions for each layer

 #Layer 1 — ZERO_RUN_SHORT (Temporal interpolation)
def impute_zero_run_short(g):
    mask = g["missing_reason"] == "ZERO_RUN_SHORT"

    g.loc[mask, "count_imputed"] = (
        g["count_imputed"]
        .interpolate(method="linear", limit=5, limit_direction="both")
    )

    g.loc[mask, "dwell_imputed"] = (
        g["dwell_imputed"]
        .interpolate(method="linear", limit=5, limit_direction="both")
    )

    g.loc[mask & g["count_imputed"].notna(), "impute_method"] = "TEMPORAL_LINEAR"
    return g


 #Layer 2 — SPIKE (Robust smoothing)
def impute_spike(g):
    mask = g["missing_reason"] == "SPIKE"

    rolling_med = (
        g["count_imputed"]
        .rolling(window=7, center=True, min_periods=3)
        .median()
    )

    g.loc[mask, "count_imputed"] = rolling_med
    g.loc[mask, "impute_method"] = "ROLLING_MEDIAN"

    return g


 #Layer 3 — PROFILE_SOFT (Time-profile based)
df["weekday"] = df["timestamp"].dt.weekday
df["minute"] = df["timestamp"].dt.hour * 60 + df["timestamp"].dt.minute
df["bucket"] = (df["minute"] // 15) * 15

profile = (
    df[df["missing_reason"] == "NONE"]
    .groupby(["sensor_id", "weekday", "bucket"])
    .agg(
        count_med=("count_clean", "median"),
        dwell_med=("dwell_clean", "median")
    )
    .reset_index()
)

def impute_profile_soft(g, profile):
    # merge first
    g = g.merge(
        profile,
        on=["sensor_id", "weekday", "bucket"],
        how="left"
    )

    # recompute mask AFTER merge
    mask = g["missing_reason"] == "PROFILE_SOFT"

    g.loc[mask, "count_imputed"] = g.loc[mask, "count_med"]
    g.loc[mask, "dwell_imputed"] = g.loc[mask, "dwell_med"]
    g.loc[mask, "impute_method"] = "PROFILE_MEDIAN"

    return g.drop(columns=["count_med", "dwell_med"])


# Apply layered imputation
out = []

for sid, g in df.groupby("sensor_id"):
    g = g.sort_values("timestamp")
    g = impute_zero_run_short(g)
    g = impute_spike(g)
    g = impute_profile_soft(g, profile)
    out.append(g)

df_imputed = pd.concat(out)

df_imputed.to_csv(
    "intersection_imputed_layered.csv",
    index=False
)
