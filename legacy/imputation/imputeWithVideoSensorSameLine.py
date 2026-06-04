# میخوایم ببینینم چقدر امکان داره سنسور های لوپ رو بر اساس سنسور های ویدئویی 
# که در همون خط قرار دارن، پر کنیم


# Find all sensors in the same line
import pandas as pd
import numpy as np

def load_lane_metadata_from_csv(meta_csv_path: str) -> pd.DataFrame:
    m = pd.read_csv(meta_csv_path)

    # normalize colnames
    m.columns = (
        m.columns.astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    # rename to standard internal names (only what we need)
    m = m.rename(columns={
        "lane_index(0-based from left to right)": "lane_index",
        "Coordinate direction": "coord_dir",
    })

    # optional filter (keep if you want)
    if "has_data_in_csv" in m.columns:
        m = m[m["has_data_in_csv"].astype(str).str.lower().eq("yes")].copy()

    # clean sensor_id
    if "sensor_id" not in m.columns:
        raise ValueError("Expected column 'sensor_id' in metadata CSV.")

    m["sensor_id"] = m["sensor_id"].astype(str).str.strip()
    m["sensor_prefix"] = m["sensor_id"].str[:1]  # D or V (or anything else)

    # ensure required fields exist
    required = ["road_name", "coord_dir", "lane_index"]
    missing = [c for c in required if c not in m.columns]
    if missing:
        raise ValueError(f"Missing required metadata columns: {missing}")

    # normalize lane_index
    m["lane_index"] = pd.to_numeric(m["lane_index"], errors="coerce").astype("Int64")

    # lane_key ONLY based on: road_name + coord_dir + lane_index
    m["lane_key"] = (
        m["road_name"].astype(str).str.strip() + " | " +
        m["coord_dir"].astype(str).str.strip() + " | " +
        m["lane_index"].astype(str)
    )

    # choose safe output columns (keep optional ones if present)
    out_cols = [
        "sensor_id", "sensor_prefix",
        "lane_key", "road_name", "lane_index", "coord_dir"
    ]

    # add optional columns if they exist (handy for debugging/joins)
    for c in ["detector_type", "intersection_id"]:
        if c in m.columns:
            out_cols.append(c)

    return m[out_cols].copy()


def sample_time_window(df: pd.DataFrame, start: str, end: str, target_rows: int = 10_000_000) -> pd.DataFrame:
    """
    window-based sampling: یک بازه زمانی انتخاب می‌کنیم که حدوداً نزدیک target_rows شود.
    """
    d = df.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce", utc=True)
    d = d.dropna(subset=["timestamp"]).copy()

    d = d[(d["timestamp"] >= pd.Timestamp(start, tz="UTC")) &
          (d["timestamp"] <  pd.Timestamp(end,   tz="UTC"))].copy()

    d = d.sort_values(["sensor_id", "timestamp"]).reset_index(drop=True)

    # اگر خیلی بزرگ شد، یک downsample ساده (بدون شکستن توالی هر سنسور)
    if len(d) > target_rows:
        idx = np.linspace(0, len(d) - 1, target_rows).astype(int)
        d = d.iloc[idx].copy().reset_index(drop=True)

    return d

# بارگذاری متادیتای خطوط
META_CSV_PATH = "/run/determined/workdir/A142_L5_20230901_complete.csv"  # مسیر CSV واقعی
meta = load_lane_metadata_from_csv(META_CSV_PATH)

print(meta.head())
print(meta["sensor_prefix"].value_counts())

def build_lane_pairs(meta: pd.DataFrame) -> pd.DataFrame:
    loops = meta[meta["sensor_prefix"] == "D"][["lane_key", "sensor_id"]].rename(columns={"sensor_id": "loop_sensor_id"})
    vids  = meta[meta["sensor_prefix"] == "V"][["lane_key", "sensor_id"]].rename(columns={"sensor_id": "video_sensor_id"})
    pairs = loops.merge(vids, on="lane_key", how="inner")
    return pairs

pairs = build_lane_pairs(meta)

print("pair rows:", len(pairs))
print("unique lanes:", pairs["lane_key"].nunique())
print(pairs.head(10))


#main data
df_test = imputed_test  # دیتای اصلی شما


# ساخت جداول مقیاس از جفت‌های لوپ-ویدئو
# برای مقایسه اینکه چقدر میشه لوپ رو از ویدئو پر کرد
# یعنی پیدا کردن یک ضریب که دیتای ویدئو رو ضرب کنیم تا به دیتای لوپ برسیم

def build_scale_tables_from_pairs(
    df: pd.DataFrame,
    pairs: pd.DataFrame,
    min_points: int = 30
):
    """
    Learn Loop ≈ scale × Video using observed overlaps.

    IMPORTANT (updated logic):
    - scale is computed as: sum(loop_count) / sum(video_count)
      (ratio-of-sums, NOT median of minute-level ratios)
    - min_points is now interpreted as minimum total VIDEO count in that group
      (i.e., sum(video_count) >= min_points), which is more stable with many zeros.

    Returns:
      g1: lane_key × weekday × bucket  (scale per group)
      g2: lane_key × bucket
      g3: lane_key
      global_scale: scalar
    """

    d = df.copy()

    # --- enforce timestamp (safer) ---
    if "timestamp" not in d.columns:
        raise ValueError("df must contain 'timestamp' column")
    d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce", utc=True)
    d = d.dropna(subset=["timestamp"]).copy()

    # --- only observed ground-truth points ---
    d = d[d["count_clean"].notna() & d["is_clean_observed"].eq(1)].copy()

    # --- time features ---
    d["weekday"] = d["timestamp"].dt.weekday
    d["minute"] = d["timestamp"].dt.hour * 60 + d["timestamp"].dt.minute
    d["bucket"] = (d["minute"] // 15) * 15

    # --- de-dup pairs to reduce accidental row explosion ---
    p = pairs[["lane_key", "loop_sensor_id", "video_sensor_id"]].drop_duplicates().copy()

    # --- loop / video mapping ---
    loops = (
        d.merge(
            p[["lane_key", "loop_sensor_id"]],
            left_on="sensor_id",
            right_on="loop_sensor_id",
            how="inner"
        )
        .rename(columns={"count_clean": "loop_count"})
        [["timestamp", "lane_key", "weekday", "bucket", "loop_count"]]
    )

    vids = (
        d.merge(
            p[["lane_key", "video_sensor_id"]],
            left_on="sensor_id",
            right_on="video_sensor_id",
            how="inner"
        )
        .rename(columns={"count_clean": "video_count"})
        [["timestamp", "lane_key", "weekday", "bucket", "video_count"]]
    )

    # --- align loop & video on same lane_key + same timestamp ---
    joined = loops.merge(
        vids,
        on=["timestamp", "lane_key", "weekday", "bucket"],
        how="inner"
    )

    # NOTE:
    # - We do NOT require loop_count>0 (zeros are informative).
    # - We do NOT compute minute-level ratio (too noisy with small counts).
    # We only need to ensure video_sum > 0 at group level.

    # ---------- Level 1 (lane_key × weekday × bucket) ----------
    g1 = (
        joined
        .groupby(["lane_key", "weekday", "bucket"], as_index=False)
        .agg(
            loop_sum=("loop_count", "sum"),
            video_sum=("video_count", "sum"),
            n=("video_count", "size")  # number of matched timestamps
        )
    )
    g1 = g1[(g1["video_sum"] >= min_points) & (g1["video_sum"] > 0)].copy()
    g1["scale"] = g1["loop_sum"] / g1["video_sum"]
    g1 = g1[["lane_key", "weekday", "bucket", "scale", "n", "loop_sum", "video_sum"]]

    # ---------- Level 2 (lane_key × bucket) ----------
    g2 = (
        joined
        .groupby(["lane_key", "bucket"], as_index=False)
        .agg(
            loop_sum=("loop_count", "sum"),
            video_sum=("video_count", "sum"),
            n=("video_count", "size")
        )
    )
    g2 = g2[(g2["video_sum"] >= min_points) & (g2["video_sum"] > 0)].copy()
    g2["scale"] = g2["loop_sum"] / g2["video_sum"]
    g2 = g2[["lane_key", "bucket", "scale", "n", "loop_sum", "video_sum"]]

    # ---------- Level 3 (lane_key) ----------
    g3 = (
        joined
        .groupby(["lane_key"], as_index=False)
        .agg(
            loop_sum=("loop_count", "sum"),
            video_sum=("video_count", "sum"),
            n=("video_count", "size")
        )
    )
    g3 = g3[(g3["video_sum"] >= min_points) & (g3["video_sum"] > 0)].copy()
    g3["scale"] = g3["loop_sum"] / g3["video_sum"]
    g3 = g3[["lane_key", "scale", "n", "loop_sum", "video_sum"]]

    # ---------- global scale ----------
    total_loop = joined["loop_count"].sum()
    total_video = joined["video_count"].sum()
    global_scale = (total_loop / total_video) if total_video > 0 else np.nan

    return g1, g2, g3, global_scale


g1, g2, g3, global_scale = build_scale_tables_from_pairs(
    df_test,
    pairs,
    min_points=30
)

print("global_scale:", global_scale)
print("L1 rows:", len(g1), "L2 rows:", len(g2), "L3 rows:", len(g3))





#ساخت روشی برای مقایسه پیک های سنسور های لوپ و ویدئویی  در باکت های یکسان در لاین های یکسان برای 
# بررسی اینکه آیا سنسور های ویدئویی پیک های یکسانی تولید میکنند و قابلیت استفاده برای ایمپوتیشن دارند یا خیر

def make_lane_bucket_timeseries(df, pairs, g1, g2, g3, global_scale, bucket_minutes=15):
    d = df.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce", utc=True)
    d = d.dropna(subset=["timestamp"]).copy()

    # فقط observed
    d = d[d["is_clean_observed"].eq(1) & d["count_clean"].notna()].copy()

    # bucket
    d["ts_bucket"] = d["timestamp"].dt.floor(f"{bucket_minutes}min")
    d["weekday"] = d["ts_bucket"].dt.weekday
    d["minute"] = d["ts_bucket"].dt.hour * 60 + d["ts_bucket"].dt.minute
    d["bucket"] = (d["minute"] // bucket_minutes) * bucket_minutes

    p = pairs[["lane_key", "loop_sensor_id", "video_sensor_id"]].drop_duplicates().copy()

    # loops
    loop_map = p[["lane_key", "loop_sensor_id"]].rename(columns={"loop_sensor_id": "sensor_id"})
    loops = (
        d.merge(loop_map, on="sensor_id", how="inner")
         .groupby(["lane_key", "ts_bucket", "weekday", "bucket"], as_index=False)
         .agg(loop=("count_clean", "sum"))
    )

    # videos
    vid_map = p[["lane_key", "video_sensor_id"]].rename(columns={"video_sensor_id": "sensor_id"})
    vids = (
        d.merge(vid_map, on="sensor_id", how="inner")
         .groupby(["lane_key", "ts_bucket", "weekday", "bucket"], as_index=False)
         .agg(video=("count_clean", "sum"))
    )

    # align
    ts = loops.merge(vids, on=["lane_key", "ts_bucket", "weekday", "bucket"], how="inner")

    # attach scale with fallback
    s1 = g1[["lane_key", "weekday", "bucket", "scale"]].rename(columns={"scale": "s1"})
    s2 = g2[["lane_key", "bucket", "scale"]].rename(columns={"scale": "s2"})
    s3 = g3[["lane_key", "scale"]].rename(columns={"scale": "s3"})

    ts = ts.merge(s1, on=["lane_key", "weekday", "bucket"], how="left")
    ts = ts.merge(s2, on=["lane_key", "bucket"], how="left")
    ts = ts.merge(s3, on=["lane_key"], how="left")

    ts["scale"] = ts["s1"].fillna(ts["s2"]).fillna(ts["s3"]).fillna(global_scale)
    ts["video_adj"] = ts["video"] * ts["scale"]

    ts["date"] = ts["ts_bucket"].dt.date
    ts["hour"] = ts["ts_bucket"].dt.hour
    return ts


def window_peak_alignment_v2(
    ts: pd.DataFrame,
    windows=((6, 10, "morning"), (15, 20, "evening")),
    tol_buckets: int = 1,
    edge_buffer_buckets: int = 1,
    min_peak_value: float = 1.0
):
    # bucket size
    if len(ts) > 1:
        bucket_minutes = int((ts["ts_bucket"].iloc[1] - ts["ts_bucket"].iloc[0]).total_seconds() / 60)
    else:
        bucket_minutes = 15
    tol_minutes = tol_buckets * bucket_minutes

    rows = []
    for (lane_key, date), g in ts.groupby(["lane_key", "date"]):
        for (start_h, end_h, tag) in windows:
            w = g[(g["hour"] >= start_h) & (g["hour"] < end_h)].sort_values("ts_bucket")
            if w.empty:
                continue

            # اگر پیک معنی‌دار نداریم، از ارزیابی حذف کن
            if (w["loop"].max() < min_peak_value) or (w["video_adj"].max() < min_peak_value):
                continue

            # حذف لبه‌ها برای جلوگیری از censoring
            if len(w) <= 2 * edge_buffer_buckets:
                continue
            w_mid = w.iloc[edge_buffer_buckets: len(w) - edge_buffer_buckets]

            t_loop = w_mid.loc[w_mid["loop"].idxmax(), "ts_bucket"]
            t_vid  = w_mid.loc[w_mid["video_adj"].idxmax(), "ts_bucket"]

            diff_min = abs((pd.Timestamp(t_loop) - pd.Timestamp(t_vid)).total_seconds()) / 60.0
            rows.append({
                "lane_key": lane_key,
                "date": date,
                "window": tag,
                "diff_minutes": diff_min,
                "aligned": diff_min <= tol_minutes
            })

    daily = pd.DataFrame(rows)
    summary = (daily.groupby(["lane_key","window"])["aligned"]
                    .agg(align_rate="mean", days="count")
                    .reset_index())
    return daily, summary


def topk_peak_overlap_in_windows(
    ts: pd.DataFrame,
    windows=((6, 10, "morning"), (15, 20, "evening")),
    k: int = 3,
    tol_buckets: int = 1,
    min_peak_value: float = 1.0
):
    if len(ts) > 1:
        bucket_minutes = int((ts["ts_bucket"].iloc[1] - ts["ts_bucket"].iloc[0]).total_seconds() / 60)
    else:
        bucket_minutes = 15
    tol_minutes = tol_buckets * bucket_minutes

    def _topk_times(w, col):
        return list(w.sort_values(col, ascending=False).head(k)["ts_bucket"].values)

    def _match(times_a, times_b):
        b = [pd.Timestamp(x) for x in times_b]
        used = set()
        matched = 0
        for ta in times_a:
            ta = pd.Timestamp(ta)
            best_j = None
            best_diff = None
            for j, tb in enumerate(b):
                if j in used:
                    continue
                diff = abs((ta - tb).total_seconds()) / 60.0
                if diff <= tol_minutes and (best_diff is None or diff < best_diff):
                    best_diff = diff
                    best_j = j
            if best_j is not None:
                used.add(best_j)
                matched += 1
        return matched, len(times_a)

    rows = []
    for (lane_key, date), g in ts.groupby(["lane_key","date"]):
        for (start_h, end_h, tag) in windows:
            w = g[(g["hour"] >= start_h) & (g["hour"] < end_h)]
            if w.empty:
                continue
            if (w["loop"].max() < min_peak_value) or (w["video_adj"].max() < min_peak_value):
                continue

            a = _topk_times(w, "loop")
            b = _topk_times(w, "video_adj")
            matched, denom = _match(a, b)

            rows.append({
                "lane_key": lane_key,
                "date": date,
                "window": tag,
                "overlap_rate": matched / denom if denom else np.nan
            })

    daily = pd.DataFrame(rows)
    summary = (daily.groupby(["lane_key","window"])["overlap_rate"]
                    .agg(rate="mean", days="count")
                    .reset_index())
    return daily, summary


def build_video_reliability_table_v2(
    ts: pd.DataFrame,
    tol_buckets: int = 1,
    k: int = 3,
    morning=(6, 10),
    evening=(15, 20),
    edge_buffer_buckets: int = 1,
    min_peak_value: float = 1.0,
    thr_window_good: float = 0.6,
    thr_topk_good: float = 0.45,   # کمی واقع‌گرایانه‌تر
):
    windows = ((morning[0], morning[1], "morning"), (evening[0], evening[1], "evening"))

    win_daily, win_summary = window_peak_alignment_v2(
        ts, windows=windows, tol_buckets=tol_buckets,
        edge_buffer_buckets=edge_buffer_buckets, min_peak_value=min_peak_value
    )

    topk_daily, topk_summary = topk_peak_overlap_in_windows(
        ts, windows=windows, k=k, tol_buckets=tol_buckets, min_peak_value=min_peak_value
    )

    # pivot align rates
    w_align = win_summary.pivot(index="lane_key", columns="window", values="align_rate").reset_index()
    w_days  = win_summary.pivot(index="lane_key", columns="window", values="days").reset_index()
    w_days.columns = ["lane_key"] + [f"days_{c}" for c in w_days.columns[1:]]

    # pivot topk
    t_rate = topk_summary.pivot(index="lane_key", columns="window", values="rate").reset_index()
    t_days = topk_summary.pivot(index="lane_key", columns="window", values="days").reset_index()
    t_days.columns = ["lane_key"] + [f"days_topk_{c}" for c in t_days.columns[1:]]

    out = (w_align
           .merge(w_days, on="lane_key", how="left")
           .merge(t_rate, on="lane_key", how="left", suffixes=("", "_topk"))
           .merge(t_days, on="lane_key", how="left"))

    # rename for clarity
    # after merge, topk columns are named "morning" and "evening" too → disambiguate:
    if "morning_topk" not in out.columns and "morning" in t_rate.columns:
        # pandas may not suffix; handle explicitly:
        pass

    # safer explicit: rebuild with known names
    out = out.rename(columns={
        "morning": "morning_align",
        "evening": "evening_align",
        "morning_topk": "morning_topk",
        "evening_topk": "evening_topk",
    })

    # if suffixing didn't happen, create them from t_rate merge result:
    if "morning_topk" not in out.columns and "morning" in t_rate.columns:
        # in some merges, columns collide; simplest: merge with explicit rename earlier
        # fallback: do it cleanly:
        pass

    # Fill missing
    for c in ["morning_align", "evening_align", "morning_topk", "evening_topk"]:
        if c in out.columns:
            out[c] = out[c].fillna(0.0)

    out["window_best_align"] = out[["morning_align", "evening_align"]].max(axis=1)
    out["window_best_topk"]  = out[["morning_topk", "evening_topk"]].max(axis=1)

    out["video_reliability"] = np.where(
        (out["window_best_align"] >= thr_window_good) & (out["window_best_topk"] >= thr_topk_good),
        "reliable_for_imputation",
        "unreliable"
    )

    return out, win_daily, topk_daily

ts = make_lane_bucket_timeseries(df_test, pairs, g1, g2, g3, global_scale, bucket_minutes=15)

reliability2, win_daily2, topk_daily2 = build_video_reliability_table_v2(
    ts,
    tol_buckets=1,
    k=3,
    morning=(6, 10),
    evening=(15, 20),
    edge_buffer_buckets=1,
    min_peak_value=1.0
)

print(reliability2.sort_values(["video_reliability", "window_best_align", "window_best_topk"]))





# بررسی همزمانی پیک های دو سنسور لوپ در دو لاین مجاور برای بررسی 
# اینکه چقدر سنسور های لوپ در لاین های مجاور پیک های همزمان دارند

def make_sensor_bucket_ts(
    df: pd.DataFrame,
    sensor_a: str,
    sensor_b: str,
    bucket_minutes: int = 15,
    value_col: str = "count_clean",
    observed_only: bool = True,
    quality_filter: bool = False
) -> pd.DataFrame:
    d = df.copy()

    d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce", utc=True)
    d = d.dropna(subset=["timestamp"]).copy()

    d["sensor_id"] = d["sensor_id"].astype(str)
    d = d[d["sensor_id"].isin([sensor_a, sensor_b])].copy()

    if value_col not in d.columns:
        raise ValueError(f"value_col='{value_col}' not in df columns")

    if observed_only:
        if "is_clean_observed" not in d.columns:
            raise ValueError("Expected column 'is_clean_observed' in df.")
        d = d[d["is_clean_observed"].eq(1)].copy()

    if quality_filter:
        for flag in ["stuck_on", "stuck_off", "cap_suspicious", "cap_flag", "profile_flag_hard"]:
            if flag in d.columns:
                d = d[d[flag].fillna(0).eq(0)].copy()

    d = d[d[value_col].notna()].copy()

    d["ts_bucket"] = d["timestamp"].dt.floor(f"{bucket_minutes}min")
    d["date"] = d["ts_bucket"].dt.date
    d["hour"] = d["ts_bucket"].dt.hour
    d["weekday"] = d["ts_bucket"].dt.weekday
    d["minute"] = d["ts_bucket"].dt.hour * 60 + d["ts_bucket"].dt.minute
    d["bucket"] = (d["minute"] // bucket_minutes) * bucket_minutes

    g = (d.groupby(["sensor_id", "ts_bucket", "date", "hour", "weekday", "bucket"], as_index=False)
           .agg(count=(value_col, "sum")))

    wide = (g.pivot(index=["ts_bucket", "date", "hour", "weekday", "bucket"],
                    columns="sensor_id",
                    values="count")
              .reset_index())

    wide = wide.dropna(subset=[sensor_a, sensor_b]).copy()
    wide = wide.rename(columns={sensor_a: "a", sensor_b: "b"})
    return wide


def window_peak_alignment_two_sensors(
    wide: pd.DataFrame,
    windows=((6, 10, "morning"), (15, 20, "evening")),
    tol_buckets: int = 1,
    edge_buffer_buckets: int = 1,
    min_peak_value: float = 1.0
):
    if wide.empty:
        return pd.DataFrame(), pd.DataFrame()

    if len(wide) > 1:
        bucket_minutes = int((wide["ts_bucket"].iloc[1] - wide["ts_bucket"].iloc[0]).total_seconds() / 60)
    else:
        bucket_minutes = 15
    tol_minutes = tol_buckets * bucket_minutes

    rows = []
    for date, g in wide.groupby("date"):
        for (start_h, end_h, tag) in windows:
            w = g[(g["hour"] >= start_h) & (g["hour"] < end_h)].sort_values("ts_bucket")
            if w.empty:
                continue
            if (w["a"].max() < min_peak_value) or (w["b"].max() < min_peak_value):
                continue
            if len(w) <= 2 * edge_buffer_buckets:
                continue

            w_mid = w.iloc[edge_buffer_buckets: len(w) - edge_buffer_buckets]
            t_a = w_mid.loc[w_mid["a"].idxmax(), "ts_bucket"]
            t_b = w_mid.loc[w_mid["b"].idxmax(), "ts_bucket"]

            diff_min = abs((pd.Timestamp(t_a) - pd.Timestamp(t_b)).total_seconds()) / 60.0
            rows.append({"date": date, "window": tag, "diff_minutes": diff_min, "aligned": diff_min <= tol_minutes})

    daily = pd.DataFrame(rows)
    summary = (daily.groupby("window")["aligned"].agg(align_rate="mean", days="count").reset_index()) if not daily.empty else pd.DataFrame()
    return daily, summary


def topk_overlap_two_sensors_in_windows(
    wide: pd.DataFrame,
    windows=((6, 10, "morning"), (15, 20, "evening")),
    k: int = 3,
    tol_buckets: int = 1,
    min_peak_value: float = 1.0
):
    if wide.empty:
        return pd.DataFrame(), pd.DataFrame()

    if len(wide) > 1:
        bucket_minutes = int((wide["ts_bucket"].iloc[1] - wide["ts_bucket"].iloc[0]).total_seconds() / 60)
    else:
        bucket_minutes = 15
    tol_minutes = tol_buckets * bucket_minutes

    def _topk_times(w, col):
        return list(w.sort_values(col, ascending=False).head(k)["ts_bucket"].values)

    def _match(times_a, times_b):
        b = [pd.Timestamp(x) for x in times_b]
        used = set()
        matched = 0
        for ta in times_a:
            ta = pd.Timestamp(ta)
            best_j, best_diff = None, None
            for j, tb in enumerate(b):
                if j in used:
                    continue
                diff = abs((ta - tb).total_seconds()) / 60.0
                if diff <= tol_minutes and (best_diff is None or diff < best_diff):
                    best_diff = diff
                    best_j = j
            if best_j is not None:
                used.add(best_j)
                matched += 1
        return matched, len(times_a)

    rows = []
    for date, g in wide.groupby("date"):
        for (start_h, end_h, tag) in windows:
            w = g[(g["hour"] >= start_h) & (g["hour"] < end_h)]
            if w.empty:
                continue
            if (w["a"].max() < min_peak_value) or (w["b"].max() < min_peak_value):
                continue

            ta = _topk_times(w, "a")
            tb = _topk_times(w, "b")
            matched, denom = _match(ta, tb)
            rows.append({"date": date, "window": tag, "overlap_rate": matched/denom if denom else np.nan})

    daily = pd.DataFrame(rows)
    summary = (daily.groupby("window")["overlap_rate"].agg(rate="mean", days="count").reset_index()) if not daily.empty else pd.DataFrame()
    return daily, summary


def ratio_of_sums_scale_two_sensors(wide: pd.DataFrame, min_b_sum: int = 30):
    if wide.empty:
        return pd.DataFrame(), np.nan

    g1 = (wide.groupby(["weekday","bucket"], as_index=False)
               .agg(a_sum=("a","sum"), b_sum=("b","sum"), n=("a","size")))

    g1 = g1[(g1["b_sum"] >= min_b_sum) & (g1["b_sum"] > 0)].copy()
    g1["scale_a_over_b"] = g1["a_sum"] / g1["b_sum"]

    total_b = wide["b"].sum()
    global_scale = wide["a"].sum() / total_b if total_b > 0 else np.nan
    return g1, global_scale


# ------------------ RUN ------------------
wide = make_sensor_bucket_ts(
    df_test,
    sensor_a="D111",
    sensor_b="D112",
    bucket_minutes=15,
    value_col="count_clean",
    observed_only=True,
    quality_filter=True
)

peak_daily, peak_sum = window_peak_alignment_two_sensors(
    wide,
    windows=((6,10,"morning"), (15,20,"evening")),
    tol_buckets=1,
    edge_buffer_buckets=1,
    min_peak_value=1.0
)

topk_daily, topk_sum = topk_overlap_two_sensors_in_windows(
    wide,
    windows=((6,10,"morning"), (15,20,"evening")),
    k=3,
    tol_buckets=1,
    min_peak_value=1.0
)

scale_tbl, global_scale = ratio_of_sums_scale_two_sensors(wide, min_b_sum=30)

print("Peak alignment summary:\n", peak_sum)
print("\nTopK overlap summary:\n", topk_sum)
print("\nGlobal scale a/b:", global_scale)
print("\nScale table sample:\n", scale_tbl.head())
