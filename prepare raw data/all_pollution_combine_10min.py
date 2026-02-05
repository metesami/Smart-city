import pandas as pd
import numpy as np
import re

# ---------- USER: set these two file paths ----------
file_dehe001 = "/content/drive/MyDrive/Test ontology_A142/Pollution/Luftmessstelle Darmstadt DEHE001/pollution_DEHE001.txt"
file_dehe040 = "/content/drive/MyDrive/Test ontology_A142/Pollution/Luftmessstelle Darmstadt Hügelstraße DEHE040/pollution_DEHE040.txt"
# --------------------------------------------------

# regex to capture a numeric token like "13,8" or "123.45"
num_re = re.compile(r'[-+]?\d+(?:[.,]\d+)?')

def extract_first_numeric_token_to_float(x):
    """Extract the first numeric-looking token from a string and convert to float.
    Treat comma as decimal separator (',' -> '.'). Return NaN if none found."""
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
    """Robustly load a pollution file:
    - read as strings (to avoid parsing surprises)
    - parse Datum + Zeit (handle '24:00')
    - extract numeric tokens from pollution columns
    Returns DataFrame with columns: datetime (naive) + original numeric-like columns (strings extracted->floats)
    """
    df = pd.read_csv(file_path, sep=';', dtype=str, encoding='utf-8', low_memory=False)

    # Require Datum and Zeit to be present
    if 'Datum' not in df.columns or 'Zeit' not in df.columns:
        raise ValueError(f"Expected 'Datum' and 'Zeit' in {file_path}. Found: {df.columns.tolist()}")

    # Prepare datetime strings, handle '24:00' -> '00:00' next day
    orig_zeit = df['Zeit'].astype(str).fillna('')
    dt_str = df['Datum'].astype(str).fillna('') + ' ' + orig_zeit.str.replace('24:00', '00:00', regex=False)
    dt_parsed = pd.to_datetime(dt_str, dayfirst=True, format='%d.%m.%Y %H:%M', errors='coerce')

    # For rows where original Zeit == '24:00', set datetime = date + 1 day
    mask_24 = orig_zeit == '24:00'
    if mask_24.any():
        dates_only = pd.to_datetime(df.loc[mask_24, 'Datum'], dayfirst=True, format='%d.%m.%Y', errors='coerce')
        dt_parsed.loc[mask_24] = dates_only + pd.Timedelta(days=1)


    # LOCALIZE to CET and CONVERT to UTC, then drop tz info for naive UTC timestamps
    dt_utc = (
        dt_parsed
        .dt.tz_localize('CET',ambiguous='NaT',            # ambiguous times (end of DST) → NaT
            nonexistent='shift_forward' # nonexistent times (start of DST) → shift forward
                        )
        .dt.tz_convert('UTC')                       # convert to UTC
        .dt.tz_localize(None)                       # drop timezone info (naive datetime in UTC)
    )

    # Build output with parsed datetime
    out = pd.DataFrame({'datetime': dt_utc})

    # Heuristic: find columns likely to be pollution numeric columns
    suspect_cols = [c for c in df.columns if any(x in c.lower() for x in ['no2','pm10','pm2','stickstoff','pm2,5'])]
    # Extract first numeric token for each suspect column
    for c in suspect_cols:
        out[c] = df[c].apply(extract_first_numeric_token_to_float)

    # Rename long original names to short names if present
    rename_map = {
        "Stickstoffdioxid (NO₂)[µg/m³]": "NO2",
        "PM10[µg/m³]": "PM10",
        "PM2,5[µg/m³]": "PM2.5"
    }
    rename_map = {k: v for k, v in rename_map.items() if k in out.columns}
    out = out.rename(columns=rename_map)

    return out


# ---------- Load both stations ----------
df1 = robust_load_pollution(file_dehe001)
df2 = robust_load_pollution(file_dehe040)


# Decide numeric columns to use (present in either df)
numeric_cols = [c for c in ["NO2", "PM10", "PM2.5"] if c in df1.columns or c in df2.columns]
if not numeric_cols:
    raise ValueError("No numeric pollution columns found after loading. Check file formats.")

# Convert datetime column to datetime dtype and drop rows without datetime
df1['datetime'] = pd.to_datetime(df1['datetime'], errors='coerce')
df2['datetime'] = pd.to_datetime(df2['datetime'], errors='coerce')
df1 = df1.dropna(subset=['datetime']).set_index('datetime').sort_index()
df2 = df2.dropna(subset=['datetime']).set_index('datetime').sort_index()


# Handle duplicate timestamps by aggregating duplicates (mean) before resampling
dups1 = df1.index.duplicated().sum()
dups2 = df2.index.duplicated().sum()
if dups1 > 0:
    print(f"Found {dups1} duplicate timestamps in station 1 -> aggregating duplicates with mean.")
    df1 = df1.groupby(level=0).mean()
if dups2 > 0:
    print(f"Found {dups2} duplicate timestamps in station 2 -> aggregating duplicates with mean.")
    df2 = df2.groupby(level=0).mean()

# Ensure numeric columns are float dtype
for c in numeric_cols:
    if c in df1.columns:
        df1[c] = pd.to_numeric(df1[c], errors='coerce')
    if c in df2.columns:
        df2[c] = pd.to_numeric(df2[c], errors='coerce')



# Resample each station separately to 10-minute bins and forward-fill up to 2 steps
limit_fill = 2  # change to 0 to disable forward-fill
df1_rs = df1[numeric_cols].resample('10T').asfreq().ffill(limit=limit_fill)
df2_rs = df2[numeric_cols].resample('10T').asfreq().ffill(limit=limit_fill)

# Combine the two resampled station frames and compute mean across available values per timestamp
combined = pd.concat([df1_rs, df2_rs], axis=0).sort_index()
agg_mean = combined.groupby(combined.index)[numeric_cols].mean()  # mean uses only non-NaN values

# Prepare final result and format datetime in ISO UTC (+00:00)
result = agg_mean.reset_index()
result['datetime'] = result['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')

# Reorder columns: datetime first, then numeric columns
cols_out = ['datetime'] + numeric_cols
result = result[cols_out]


# Save final CSV (no StationID, no n_sources)
out_file = 'pollution_10min.csv'
result.to_csv(out_file, index=False)

print("Saved final averaged file:", out_file)
print(result.head(12).to_string(index=False))
