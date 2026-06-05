def compute_flags_for_sensor(df_sensor: pd.DataFrame,
                             use_empirical_cap: bool = True) -> pd.DataFrame:
    """
    df_sensor: ['timestamp','sensor_id','count_raw','dwell_raw']
    Returns: same + *_clean + flag columns (pre-fusion cleaning)
    """
    out = df_sensor.copy().sort_values("timestamp").reset_index(drop=True)

    # ---------- S1: یکنواختی زمانی (بدون minute_diff_ok) ----------
    # اختلاف زمانی با ردیف قبل به ثانیه
    deltas = out["timestamp"].diff().dt.total_seconds()
    # gap وقتی است که اختلاف != 60 و مقدار قبلی داریم
    out["gap_flag"] = ((deltas.notna()) & (~deltas.eq(60.0))).astype(int)
    # overlap/duplicate (احتیاطاً اگر تایم تکراری داریم)
    out["overlap_flag"] = out["timestamp"].duplicated(keep="first").astype(int)

    # ---------- S2: چک‌های فیزیکی/منطقی ----------
    out["phys_flag"], out["logic_flag"] = 0, 0

    phys_invalid = (
        (out["count_raw"] < 0) |
        (out["dwell_raw"] < 0) |
        (out["dwell_raw"] > CONFIG["dwell_abs_max_ms"])
    )
    out.loc[phys_invalid, "phys_flag"] = 1

    # count=0 اما dwell>0 -> منطقی نیست؛ فلگ و نرمال‌سازی بعداً
    cond = (out["count_raw"] == 0) & (out["dwell_raw"] > 0)
    out.loc[cond, "logic_flag"] = 1

    out["avg_dwell"] = np.where(
        out["count_raw"] > 0,
        out["dwell_raw"] / out["count_raw"].clip(lower=1),
        np.nan
    )
    avg_bad = (
        (out["count_raw"] > 0) &
        ((out["avg_dwell"] < CONFIG["avg_dwell_min_ms"]) |
         (out["avg_dwell"] > CONFIG["avg_dwell_max_ms"]))
    )
    out.loc[avg_bad, "logic_flag"] = 1

    # ---------- S3: سقف ظرفیت دقیقه‌ای ----------
    if use_empirical_cap:
        # نسخهٔ تجربی: P99 بر اساس (weekday × hour) و کپ با سقف جهانی
        cap_series = empirical_cap(out["count_raw"], out["timestamp"])
        out["C_cap_min"] = np.minimum(cap_series, CONFIG["global_cap_per_minute"])
        out["cap_flag"] = (out["count_raw"] > out["C_cap_min"]).astype(int)
        # ستون «مظنون» اختیاری (۵٪ آخر زیر سقف)
        out["cap_suspicious"] = (
            (out["count_raw"] > 0.9 * out["C_cap_min"]) &
            (out["count_raw"] <= out["C_cap_min"])
        ).astype(int)
    else:
        # نسخهٔ ثابت (در صورت تمایل)
        gl_cap = float(CONFIG["global_cap_per_minute"])
        out["C_cap_min"] = gl_cap
        out["cap_flag"] = (out["count_raw"] > gl_cap).astype(int)
        # اگر cap_suspicious_lower تعریف نشده بود، یک مقدار پیش‌فرض بساز
        susp_low = float(CONFIG.get("cap_suspicious_lower", max(0.0, gl_cap * 0.9)))
        out["cap_suspicious"] = (
            (out["count_raw"] > susp_low) & (out["count_raw"] <= gl_cap)
        ).astype(int)

    # ---------- S4: خرابی‌های کلاسیک و جهش ----------
    zero_mask = (out["count_raw"] == 0) & (out["dwell_raw"] == 0)

    w0 = CONFIG["zero_run_minutes"]
    out["zero_run"] = zero_mask.rolling(w0, min_periods=w0).sum().fillna(0).ge(w0).astype(int)

    w_off = CONFIG["stuck_off_minutes"]
    out["stuck_off"] = zero_mask.rolling(w_off, min_periods=w_off).sum().fillna(0).ge(w_off).astype(int)

    w_on = CONFIG["stuck_on_minutes"]
    rolling_std = out["count_raw"].rolling(w_on, min_periods=w_on).std()
    rolling_mean = out["count_raw"].rolling(w_on, min_periods=w_on).mean()
    out["stuck_on"] = ((rolling_std.fillna(np.inf) < 1e-6) & (rolling_mean.fillna(0) > 0)).astype(int)

    N = CONFIG["spike_window_minutes"]
    diffs = out["count_raw"].diff().values
    mad_vals = np.full_like(diffs, np.nan, dtype=float)
    for i in range(len(diffs)):
        j0 = max(0, i - N + 1)
        wv = diffs[j0:i+1]
        if len(wv) >= 5:
            m = np.nanmedian(wv); mad = np.nanmedian(np.abs(wv - m))
            mad_vals[i] = mad
    out["spike_flag"] = 0
    thr_mult = CONFIG["spike_mad_multiplier"]
    with np.errstate(invalid='ignore'):
        out.loc[(np.abs(diffs) > thr_mult * mad_vals), "spike_flag"] = 1

    q75 = out["count_raw"].rolling(60, min_periods=20).quantile(0.75)
    q10 = out["count_raw"].rolling(60, min_periods=20).quantile(0.10)
    out["cliff_flag"] = ((out["count_raw"].shift(1) > q75) & (out["count_raw"] < q10)).astype(int)

    # ---------- S5: ناسازگاری Flow–Dwell (نسخهٔ سبک) ----------
    roll_p90_count = out["count_raw"].rolling(120, min_periods=30).quantile(0.90)
    roll_p90_avgdw = out["avg_dwell"].rolling(120, min_periods=30).quantile(0.90)
    out["cd_inconsistency"] = ((out["count_raw"] > roll_p90_count) &
                               (out["avg_dwell"] > roll_p90_avgdw)).astype(int)

    # ---------- S6: پروفایل تاریخی مقاوم ----------
    prof_c = build_time_profiles(out["timestamp"], out["count_raw"], profile_minutes=CONFIG["profile_group_minutes"])
    prof_d = build_time_profiles(out["timestamp"], out["dwell_raw"], profile_minutes=CONFIG["profile_group_minutes"])

    dt = out["timestamp"].dt
    minutes = dt.hour * 60 + dt.minute
    bucket = (minutes // CONFIG["profile_group_minutes"]) * CONFIG["profile_group_minutes"]
    idx = list(zip(dt.weekday.values, bucket.values))

    def map_prof(prof_df, arr_idx, col):
        vals = []
        for w,b in arr_idx:
            try:
                vals.append(prof_df.loc[(w,b), col])
            except KeyError:
                vals.append(np.nan)
        return np.array(vals, dtype=float)

    c_med = map_prof(prof_c, idx, "median")
    c_iqr = map_prof(prof_c, idx, "iqr")
    d_med = map_prof(prof_d, idx, "median")
    d_iqr = map_prof(prof_d, idx, "iqr")

    def robust_z(x, med, iqr):
        denom = np.where(iqr > 0, iqr / 1.349, np.nan)
        with np.errstate(divide='ignore', invalid='ignore'):
            return (x - med) / denom

    zc = robust_z(out["count_raw"].values.astype(float), c_med, c_iqr)
    zd = robust_z(out["dwell_raw"].values.astype(float), d_med, d_iqr)
    out["profile_z_count"] = zc
    out["profile_z_dwell"] = zd
    out["profile_flag_soft"] = ((np.abs(zc) > CONFIG["profile_soft_z"]) |
                                (np.abs(zd) > CONFIG["profile_soft_z"])).astype(int)
    out["profile_flag_hard"] = ((np.abs(zc) > CONFIG["profile_hard_z"]) |
                                (np.abs(zd) > CONFIG["profile_hard_z"])).astype(int)

    # ---------- S7: تولید سری‌های clean ----------
    hard_flags = (
        (out["gap_flag"] == 1) |
        (out["phys_flag"] == 1) |
        (out["cap_flag"] == 1) |
        (out["stuck_off"] == 1) |
        (out["stuck_on"] == 1) |
        (out["zero_run"] == 1) |
        (out["spike_flag"] == 1) |
        (out["cliff_flag"] == 1) |
        (out["cd_inconsistency"] == 1) |
        (out["profile_flag_hard"] == 1)
    )

    out["count_clean"] = out["count_raw"].astype(float).copy()
    out["dwell_clean"] = out["dwell_raw"].astype(float).copy()
    out.loc[hard_flags, ["count_clean", "dwell_clean"]] = np.nan

    # منطق: اگر count_clean==0 ولی dwell_clean>0، dwell_clean را 0 کن
    fix_cond = (out["count_clean"] == 0) & (out["dwell_clean"] > 0)
    out.loc[fix_cond, "dwell_clean"] = 0.0

    return out
