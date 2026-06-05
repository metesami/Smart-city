import pandas as pd
import numpy as np
import re

# ---------- USER: set these two file paths ----------
file_dehe001 = "/run/determined/workdir/DEHE001.txt"
file_dehe040 = "/run/determined/workdir/DEHE040.txt"
# --------------------------------------------------

num_re = re.compile(r'[-+]?\d+(?:[.,]\d+)?')

def extract_first_numeric_token_to_float(x):
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    m = num_re.search(s)
    if not m:
        return np.nan
    token = m.group(0).replace(',', '.')
    try:
        return float(token)
    except:
        return np.nan

def robust_load_pollution(file_path):
    df = pd.read_csv(file_path, sep=';', dtype=str, encoding='utf-8', low_memory=False)

    if 'Datum' not in df.columns or 'Zeit' not in df.columns:
        raise ValueError(f"Expected 'Datum' and 'Zeit' in {file_path}. Found: {df.columns.tolist()}")

    # handle 24:00
    orig_zeit = df['Zeit'].astype(str).fillna('')
    dt_str = df['Datum'].astype(str).fillna('') + ' ' + orig_zeit.str.replace('24:00', '00:00', regex=False)
    dt_parsed = pd.to_datetime(dt_str, dayfirst=True, format='%d.%m.%Y %H:%M', errors='coerce')

    mask_24 = orig_zeit == '24:00'
    if mask_24.any():
        dates_only = pd.to_datetime(df.loc[mask_24, 'Datum'], dayfirst=True, format='%d.%m.%Y', errors='coerce')
        dt_parsed.loc[mask_24] = dates_only + pd.Timedelta(days=1)

    # CET -> UTC -> naive (UTC)
    dt_utc = (
        dt_parsed
        .dt.tz_localize('CET', ambiguous='NaT', nonexistent='shift_forward')
        .dt.tz_convert('UTC')
        .dt.tz_localize(None)
    )

    out = pd.DataFrame({'datetime': dt_utc})

    suspect_cols = [c for c in df.columns if any(x in c.lower() for x in ['no2','pm10','pm2','stickstoff','pm2,5'])]
    for c in suspect_cols:
        out[c] = df[c].apply(extract_first_numeric_token_to_float)

    rename_map = {
        "Stickstoffdioxid (NO₂)[µg/m³]": "NO2",
        "PM10[µg/m³]": "PM10",
        "PM2,5[µg/m³]": "PM2.5"
    }
    rename_map = {k: v for k, v in rename_map.items() if k in out.columns}
    out = out.rename(columns=rename_map)

    return out

def to_10min_station(df, station_id, numeric_cols=("NO2","PM10","PM2.5"), limit_fill=2):
    df = df.copy()
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df = df.dropna(subset=['datetime']).set_index('datetime').sort_index()

    # duplicates داخل همان ایستگاه
    if df.index.duplicated().any():
        df = df.groupby(level=0).mean(numeric_only=True)

    keep_cols = [c for c in numeric_cols if c in df.columns]
    for c in keep_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    # 10-min grid
    df_rs = df[keep_cols].resample('10T').asfreq()

    # optional: short forward-fill
    if limit_fill and limit_fill > 0:
        df_rs = df_rs.ffill(limit=limit_fill)

    df_rs['StationID'] = station_id
    return df_rs.reset_index()

# ---------- Build 10-min raw data for BOTH stations (NO aggregation across stations) ----------
df1 = robust_load_pollution(file_dehe001)
df2 = robust_load_pollution(file_dehe040)

rs1 = to_10min_station(df1, "DEHE001", limit_fill=2)
rs2 = to_10min_station(df2, "DEHE040", limit_fill=2)

pollution_10min = pd.concat([rs1, rs2], ignore_index=True)

# ensure datetime is proper
pollution_10min['datetime'] = pd.to_datetime(pollution_10min['datetime'], errors='coerce')
pollution_10min = pollution_10min.dropna(subset=['datetime'])

# timestamp_seconds مثل قبل
pollution_10min['timestamp_seconds'] = pollution_10min['datetime'].astype('int64') // 10**9

# ذخیره datetime به فرمت ISO با +00:00 (برای اینکه بعداً parse_dates هم راحت باشه)
pollution_10min['datetime'] = pollution_10min['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')

# order columns
pollutants = [c for c in ['NO2','PM10','PM2.5'] if c in pollution_10min.columns]
pollution_10min = pollution_10min[['datetime','timestamp_seconds','StationID'] + pollutants] \
    .sort_values(['StationID','timestamp_seconds'])

out_file = '/run/determined/workdir/pollution_10min_by_station.csv'
pollution_10min.to_csv(out_file, index=False)

print("✅ Saved:", out_file)
print(pollution_10min.head(20).to_string(index=False))
