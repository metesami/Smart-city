
#----------- conver some collumns of result in .parquet to .csv ----------

import os
import pandas as pd
from typing import List

# تنظیمات
intersection_id = "A142"
in_dir = "/run/determined/workdir/Cleaned_data"   # مسیر پوشه‌ای که فایل پارکت داخلشه — اگر جای دیگه است این را تغییر بده
parquet_path = os.path.join(in_dir, f"{intersection_id}_clean_pre_fusion.parquet")
csv_fallback = os.path.join(in_dir, f"{intersection_id}_clean_pre_fusion.csv.gz")
out_csv = os.path.join(in_dir, "imputation_test.csv")

# لیست سنسورهایی که می‌خوای جدا کنی
SENSORS: List[str] = ["V92", "V84", "D82", "D91"]

# helper: خواندن parquet یا fallback به csv.gz
def load_result(path_parquet: str, path_csv_gz: str) -> pd.DataFrame:
    if os.path.exists(path_parquet):
        print(f"Loading parquet: {path_parquet}")
        return pd.read_parquet(path_parquet)
    elif os.path.exists(path_csv_gz):
        print(f"Parquet not found — loading CSV fallback: {path_csv_gz}")
        return pd.read_csv(path_csv_gz, parse_dates=["timestamp"])
    else:
        raise FileNotFoundError(f"Neither {path_parquet} nor {path_csv_gz} found.")

# بارگذاری
df = load_result(parquet_path, csv_fallback)

# انتظار داریم df شامل ستون‌هایی مثل: timestamp, sensor_id, count_clean, dwell_clean
# بررسی اولیه
required_cols = {"timestamp", "sensor_id", "count_clean", "dwell_clean"}
if not required_cols.issubset(set(df.columns)):
    raise ValueError(f"DataFrame missing required columns. Found columns: {df.columns.tolist()}")

# فیلتر سنسورهای مورد نظر
df_sel = df[df["sensor_id"].isin(SENSORS)].copy()
if df_sel.empty:
    raise ValueError(f"No rows found for sensors {SENSORS} in the file.")

# اطمینان از نوع timestamp
df_sel["timestamp"] = pd.to_datetime(df_sel["timestamp"], errors="coerce")

# pivot به wide: هر سنسور دو ستون جدا (مثلاً V92_count_clean, V92_dwell_clean)
df_wide_count = df_sel.pivot_table(index="timestamp", columns="sensor_id", values="count_clean", aggfunc="first")
df_wide_dwell = df_sel.pivot_table(index="timestamp", columns="sensor_id", values="dwell_clean", aggfunc="first")

# مرتب و نامگذاری ستون‌ها
df_wide_count = df_wide_count.rename(columns=lambda s: f"{s}_count_clean")
df_wide_dwell = df_wide_dwell.rename(columns=lambda s: f"{s}_dwell_clean")

# join دو ماتریس کنار هم
df_wide = pd.concat([df_wide_count, df_wide_dwell], axis=1)

# اگر می‌خواهی ستون timestamp هم به عنوان ستون عادی باشد (نه ایندکس)
df_wide = df_wide.reset_index().sort_values("timestamp")

# گزینه: مرتب کردن ستون‌ها بر اساس لیست SENSORS تا زوج‌ها پیوست باشند
ordered_cols = ["timestamp"]
for s in SENSORS:
    ordered_cols.append(f"{s}_count_clean")
    ordered_cols.append(f"{s}_dwell_clean")
# اضافه کردن هر ستونی که وجود دارد ولی در ordered_cols نیامده (برای ایمنی)
remaining = [c for c in df_wide.columns if c not in ordered_cols]
final_cols = [c for c in ordered_cols if c in df_wide.columns] + remaining
df_wide = df_wide[final_cols]

# ذخیره CSV نهایی (بدون فشرده‌سازی، برای استفاده راحت در آزمایش مدل‌ها)
df_wide.to_csv(out_csv, index=False)
print(f"Saved imputation test CSV: {out_csv}")
