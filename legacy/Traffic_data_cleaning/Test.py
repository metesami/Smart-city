# Test number 4: explained in zoterio in folder traffic data cleaning
import pandas as pd
clean = pd.read_parquet("/run/determined/workdir/clean_traffic_data/A142_clean_pre_fusion.parquet") #clean data after applying second_version.py
raw = pd.read_csv("/run/determined/workdir/A142_traffic_1min.csv") #raw data before cleaning

raw_long = []

for sensor in ["D122","V124","D121","V123",
 "D112","V114","D111","V113",
 "D91","V92","V84","D82",
 "V83","D81","D62","V64",
 "D61","V63","D52","V54",
 "D51","V53","D31","V32",
 "V26","D23","V25","D22",
 "V24","D21"]:  # همه سنسورها
    df_temp = raw[["Intervallbeginn (UTC)",
                   f"{sensor} (Belegungen/Intervall)",
                   f"{sensor} (Verweilzeit/Intervall) [ms]"]].copy()
    df_temp["sensor_id"] = sensor
    df_temp.rename(columns={
        "Intervallbeginn (UTC)": "timestamp",
        f"{sensor} (Belegungen/Intervall)": "count_raw",
        f"{sensor} (Verweilzeit/Intervall) [ms]": "dwell_raw"
    }, inplace=True)
    raw_long.append(df_temp)

raw_long = pd.concat(raw_long, ignore_index=True)

raw_long["zero_raw"] = (raw_long.count_raw==0) & (raw_long.dwell_raw==0)

# شمارش صفرها در پنجره‌های 5 دقیقه و 20 دقیقه
raw_long["zero_run5"] = raw_long.groupby("sensor_id")["zero_raw"].rolling(5).sum().reset_index(0, drop=True)
raw_long["zero_run20"] = raw_long.groupby("sensor_id")["zero_raw"].rolling(20).sum().reset_index(0, drop=True)

# تبدیل timestamp به نوع datetime
raw_long['timestamp'] = pd.to_datetime(raw_long['timestamp'], errors='coerce', utc=True)

# ادغام داده‌های پاک‌شده با شمارش صفرها
test = clean.merge(
    raw_long[["timestamp","sensor_id","zero_run5","zero_run20"]],
    on=["timestamp","sensor_id"],
    how="left"
)

# فیلتر کردن رکوردهایی که در آن‌ها zero_run20 برابر با 20 است و stuck_off برابر با 1 نیست
test[ (test.zero_run20==20) & (test.stuck_off!=1) ]




# Test number 5:
