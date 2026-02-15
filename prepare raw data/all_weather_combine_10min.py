import numpy as np
import pandas as pd

# -------- paths (adjust if needed) --------
meta_path = "/run/determined/workdir/weather_stations_metadata.csv"

temp_path   = "/run/determined/workdir/tempreture-2022 to 2024.txt"
precip_path = "/run/determined/workdir/precipitation-2022 to 2024.txt"
press_path  = "/run/determined/workdir/pressure-2022 to 2024.txt"
wind_path   = "/run/determined/workdir/wind-2022 to 2024.txt"

out_path = "/run/determined/workdir/weather_10min_by_station.csv"
# -----------------------------------------

# --- get StationID from metadata (like pollution) ---
meta = pd.read_csv(meta_path, encoding="utf-8", low_memory=False)
if "StationID" not in meta.columns or meta.empty:
    raise ValueError("weather_stations_metadata.csv must contain a StationID column with at least one row.")
STATION_ID = str(meta.iloc[0]["StationID"]).strip()

def parse_mess_datum(series: pd.Series) -> pd.DatetimeIndex:
    """
    Robust parse for MESS_DATUM:
    - if length >= 12 => YYYYMMDDHHMM
    - else => YYYYMMDDHH
    Treat as UTC and return tz-aware UTC timestamps.
    """
    s = series.astype(str).str.strip()
    sample = s.dropna().iloc[0] if s.dropna().shape[0] else ""
    fmt = "%Y%m%d%H%M" if len(sample) >= 12 else "%Y%m%d%H"
    ts = pd.to_datetime(s, format=fmt, errors="coerce", utc=True)
    return ts

def clean_numeric(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            df[c] = df[c].replace(-999, np.nan)
    return df

# -------- Temperature + Humidity (10-min) --------
df_temp = pd.read_csv(temp_path, sep=";", decimal=",", encoding="utf-8", low_memory=False)
df_temp["datetime"] = parse_mess_datum(df_temp["MESS_DATUM"])
df_temp = df_temp.dropna(subset=["datetime"]).set_index("datetime").sort_index()

keep = [c for c in ["TT_10", "RF_10"] if c in df_temp.columns]
df_temp = df_temp[keep].copy()
df_temp = clean_numeric(df_temp, keep)

# rename to RAW names you want to store
rename_map = {"TT_10": "temperature", "RF_10": "humidity"}
df_temp = df_temp.rename(columns={k: v for k, v in rename_map.items() if k in df_temp.columns})

# -------- Precipitation (10-min) --------
df_prec = pd.read_csv(precip_path, sep=";", decimal=",", encoding="utf-8", low_memory=False)
df_prec["datetime"] = parse_mess_datum(df_prec["MESS_DATUM"])
df_prec = df_prec.dropna(subset=["datetime"]).set_index("datetime").sort_index()

prec_cols = [c for c in ["RWS_10", "RWS_IND_10"] if c in df_prec.columns]
df_prec = df_prec[prec_cols].copy()
df_prec = clean_numeric(df_prec, prec_cols)

# store precipitation in mm for last 10 min
if "RWS_10" in df_prec.columns:
    df_prec = df_prec.rename(columns={"RWS_10": "precipitation"})
if "RWS_IND_10" in df_prec.columns:
    # rain flag 0/1
    df_prec["rain_flag"] = df_prec["RWS_IND_10"].fillna(np.nan)
    df_prec = df_prec.drop(columns=["RWS_IND_10"])

# -------- Pressure (hourly -> 10-min) --------
df_press = pd.read_csv(press_path, sep=";", decimal=",", encoding="utf-8", low_memory=False)
df_press["datetime"] = parse_mess_datum(df_press["MESS_DATUM"])
df_press = df_press.dropna(subset=["datetime"]).set_index("datetime").sort_index()

# your file had "  P0" (with spaces). handle both possibilities:
if "  P0" in df_press.columns:
    df_press = df_press[["  P0"]].rename(columns={"  P0": "pressure"})
elif "P0" in df_press.columns:
    df_press = df_press[["P0"]].rename(columns={"P0": "pressure"})
elif "PP_10" in df_press.columns:
    df_press = df_press[["PP_10"]].rename(columns={"PP_10": "pressure"})
else:
    # fallback: take first numeric-like column
    df_press = df_press.iloc[:, :1].rename(columns={df_press.columns[0]: "pressure"})

df_press = clean_numeric(df_press, ["pressure"])

# if pressure is hourly, make it 10-min with ffill up to 5 steps (50 minutes)
# also extend last hour to avoid trailing NaNs after resample
last_time = df_press.index.max()
if pd.notna(last_time):
    df_press.loc[last_time + pd.Timedelta(hours=1)] = df_press.iloc[-1].values

df_press = df_press.resample("10T").asfreq().ffill(limit=5)

# -------- Wind (10-min) --------
df_wind = pd.read_csv(wind_path, sep=";", decimal=",", encoding="utf-8", low_memory=False)
df_wind["datetime"] = parse_mess_datum(df_wind["MESS_DATUM"])
df_wind = df_wind.dropna(subset=["datetime"]).set_index("datetime").sort_index()

wind_keep = [c for c in ["FF_10", "DD_10"] if c in df_wind.columns]
df_wind = df_wind[wind_keep].copy()
df_wind = clean_numeric(df_wind, wind_keep)

# rename to RAW names
wind_rename = {"FF_10": "wind_speed", "DD_10": "wind_direction"}
df_wind = df_wind.rename(columns={k: v for k, v in wind_rename.items() if k in df_wind.columns})

# optional: forward-fill wind gaps
for c in ["wind_speed", "wind_direction"]:
    if c in df_wind.columns:
        df_wind[c] = df_wind[c].ffill()

# -------- Merge everything on datetime index --------
dfs = [df_temp, df_prec, df_press, df_wind]
merged = pd.concat(dfs, axis=1, join="outer").sort_index()

# build final result
result = merged.reset_index().rename(columns={"index": "datetime"})
result["datetime"] = pd.to_datetime(result["datetime"], utc=True, errors="coerce")
result = result.dropna(subset=["datetime"])

# timestamp_seconds from UTC datetime
result["timestamp_seconds"] = (result["datetime"].astype("int64") // 10**9).astype("int64")

# StationID column
result["StationID"] = STATION_ID

# datetime to ISO +00:00
result["datetime"] = result["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

# order columns (keep only those present)
base = ["datetime", "timestamp_seconds", "StationID"]
vals = [c for c in ["temperature", "humidity", "pressure", "precipitation", "wind_speed", "wind_direction", "rain_flag"] if c in result.columns]
result = result[base + vals].sort_values(["StationID", "timestamp_seconds"])

result.to_csv(out_path, index=False, encoding="utf-8")
print("✅ Saved:", out_path)
print(result.head(20).to_string(index=False))
