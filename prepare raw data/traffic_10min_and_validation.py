# This script reads the raw imputed layered data,
#  performs 10-minute aggregation and feature engineering,
#  and saves the result as a new Parquet file.

pip install duckdb

import duckdb

IN_CSV = "intersection_imputed_layered.csv"
OUT_PARQUET = "intersection_imputed_layered_10min_analytics.parquet"

con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=8;")
con.execute("PRAGMA enable_progress_bar;")

query = f"""
COPY (
  WITH base AS (
    SELECT
      sensor_id,

      -- 10-minute bin via epoch (portable)
      to_timestamp(
        FLOOR(EXTRACT(EPOCH FROM CAST(timestamp AS TIMESTAMPTZ)) / 600) * 600
      ) AS timestamp,

      TRY_CAST(count_imputed AS DOUBLE) AS count_imputed,
      TRY_CAST(dwell_imputed AS DOUBLE) AS dwell_imputed,

      -- confidence can be 'none'/'None'/'' etc -> TRY_CAST -> NULL
      TRY_CAST(NULLIF(TRIM(CAST(confidence AS VARCHAR)), '') AS DOUBLE) AS confidence,

      CASE WHEN COALESCE(soft_flag, 0) IN (1, TRUE) THEN 1 ELSE 0 END AS soft_flag,
      CASE WHEN COALESCE(profile_flag_hard, 0) IN (1, TRUE) THEN 1 ELSE 0 END AS profile_flag_hard,
      CASE WHEN COALESCE(spike_flag, 0) IN (1, TRUE) THEN 1 ELSE 0 END AS spike_flag,

      CASE WHEN COALESCE(is_clean_observed, 0) IN (1, TRUE) THEN 1 ELSE 0 END AS is_clean_observed,
      CASE WHEN COALESCE(imputable, 0) IN (1, TRUE) THEN 1 ELSE 0 END AS imputable,

      NULLIF(TRIM(CAST(impute_method AS VARCHAR)), '')  AS impute_method,
      NULLIF(TRIM(CAST(missing_reason AS VARCHAR)), '') AS missing_reason

    FROM read_csv_auto('{IN_CSV}', ignore_errors=true)
    WHERE timestamp IS NOT NULL AND sensor_id IS NOT NULL
  ),

  agg AS (
    SELECT
      sensor_id,
      timestamp,

      AVG(CASE WHEN count_imputed IS NULL THEN 0 ELSE 1 END) AS coverage_count,
      AVG(CASE WHEN dwell_imputed IS NULL THEN 0 ELSE 1 END) AS coverage_dwell,

      SUM(count_imputed) FILTER (WHERE count_imputed IS NOT NULL) AS count_10min,

      -- rename to semantic meaning: occupancy time (ms) in 10-min interval
      SUM(dwell_imputed) FILTER (WHERE dwell_imputed IS NOT NULL) AS occupancy_time_10min,

      MIN(confidence) FILTER (WHERE confidence IS NOT NULL) AS confidence_min,
      AVG(confidence) FILTER (WHERE confidence IS NOT NULL) AS confidence_mean,

      AVG(is_clean_observed) AS is_clean_observed_rate,
      AVG(imputable)         AS imputable_rate,

      MAX(soft_flag)         AS soft_flag_any,
      AVG(soft_flag)         AS soft_flag_rate,

      MAX(profile_flag_hard) AS profile_flag_hard_any,
      AVG(profile_flag_hard) AS profile_flag_hard_rate,

      MAX(spike_flag)        AS spike_flag_any,
      AVG(spike_flag)        AS spike_flag_rate,

      MODE(impute_method)  AS impute_method_mode,
      COUNT(DISTINCT impute_method) FILTER (WHERE impute_method IS NOT NULL)
        AS impute_method_nunique,

      MODE(missing_reason) AS missing_reason_mode,
      COUNT(DISTINCT missing_reason) FILTER (WHERE missing_reason IS NOT NULL)
        AS missing_reason_nunique

    FROM base
    GROUP BY sensor_id, timestamp
  )

  SELECT
    *,
    -- new: imputed_rate
    (1.0 - is_clean_observed_rate) AS imputed_rate,

    -- new: occupancy ratio in the 10-min interval (600,000 ms)
    CASE
      WHEN occupancy_time_10min IS NOT NULL
      THEN occupancy_time_10min / 600000.0
      ELSE NULL
    END AS occupancy_ratio_10min,

    EXTRACT(dow FROM timestamp) AS weekday,
    (EXTRACT(hour FROM timestamp)*60 + EXTRACT(minute FROM timestamp))::INTEGER AS minute,
    ((EXTRACT(hour FROM timestamp)*60 + EXTRACT(minute FROM timestamp))::INTEGER / 10) * 10 AS bucket
  FROM agg
) TO '{OUT_PARQUET}' (FORMAT PARQUET);
"""

con.execute(query)
print("✅ Saved:", OUT_PARQUET)



# convert the parquet to csv for easier loading in some tools 
import duckdb

PARQUET_FILE = "intersection_imputed_layered_10min_analytics.parquet"
OUT_CSV      = "intersection_imputed_layered_10min_analytics.csv"

con = duckdb.connect()
con.execute(f"""
COPY (SELECT * FROM '{PARQUET_FILE}')
TO '{OUT_CSV}' (HEADER, DELIMITER ',');
""")

print("✅ CSV written:", OUT_CSV)



##Validity Test

# Alignment Check
import duckdb

P = "intersection_imputed_layered_10min_analytics.parquet"
con = duckdb.connect()

con.execute(f"""
SELECT DISTINCT EXTRACT(minute FROM timestamp) AS m
FROM '{P}'
ORDER BY m
""").fetchall()

# Check 10 min sum comparison minutely data

sid = con.execute(f"SELECT sensor_id FROM '{P}' USING SAMPLE 1").fetchone()[0]
row = con.execute(f"""
SELECT sensor_id, timestamp
FROM '{P}'
WHERE sensor_id = '{sid}'
ORDER BY random()
LIMIT 1
""").fetchone()

sid, ts10 = row
print("sensor:", sid, "ts10:", ts10)


# Compare minute sum with 10-minute value (count and occupancy_time)

MINUTE_CSV = "intersection_imputed_layered.csv"

# sum minute-level over the same 10-minute window
res_minute = con.execute(f"""
WITH m AS (
  SELECT
    sensor_id,
    to_timestamp(FLOOR(EXTRACT(EPOCH FROM CAST(timestamp AS TIMESTAMPTZ)) / 600) * 600) AS ts10,
    TRY_CAST(count_imputed AS DOUBLE) AS c,
    TRY_CAST(dwell_imputed AS DOUBLE) AS o
  FROM read_csv_auto('{MINUTE_CSV}', ignore_errors=true)
  WHERE sensor_id = '{sid}'
)
SELECT
  ts10,
  SUM(c) FILTER (WHERE c IS NOT NULL) AS count_sum_minute,
  SUM(o) FILTER (WHERE o IS NOT NULL) AS occ_sum_minute
FROM m
WHERE ts10 = TIMESTAMPTZ '{ts10}'
GROUP BY ts10
""").fetchone()

res_10 = con.execute(f"""
SELECT
  timestamp,
  count_10min,
  occupancy_time_10min
FROM '{P}'
WHERE sensor_id = '{sid}' AND timestamp = TIMESTAMPTZ '{ts10}'
""").fetchone()

print("minute sums:", res_minute)
print("10min row  :", res_10)

'''
#ecpectation:
count_sum_minute ≈ count_10min

occ_sum_minute ≈ occupancy_time_10min
'''


#coverage test: This tells you how many of the 10-mins are actually made up of 10 full minutes.

con.execute(f"""
SELECT
  AVG(CASE WHEN coverage_count = 1 THEN 1 ELSE 0 END) AS frac_full_count,
  AVG(CASE WHEN coverage_dwell = 1 THEN 1 ELSE 0 END) AS frac_full_dwell
FROM '{P}'
""").fetchone()


#Percentage of NaN/zero/positive (on 10-minute parquet file)

import duckdb
P = "intersection_imputed_layered_10min_analytics.parquet"
con = duckdb.connect()

con.execute(f"""
SELECT
  COUNT(*) AS n,

  -- count_10min
  AVG(CASE WHEN count_10min IS NULL THEN 1 ELSE 0 END) AS count_nan_rate,
  AVG(CASE WHEN count_10min = 0 THEN 1 ELSE 0 END)     AS count_zero_rate,
  AVG(CASE WHEN count_10min > 0 THEN 1 ELSE 0 END)     AS count_pos_rate,

  -- occupancy_time_10min
  AVG(CASE WHEN occupancy_time_10min IS NULL THEN 1 ELSE 0 END) AS occ_nan_rate,
  AVG(CASE WHEN occupancy_time_10min = 0 THEN 1 ELSE 0 END)     AS occ_zero_rate,
  AVG(CASE WHEN occupancy_time_10min > 0 THEN 1 ELSE 0 END)     AS occ_pos_rate

FROM '{P}';
""").fetchone()


#Only on rows that "actually have data" (i.e. remove NaN) Zero and Positive

con.execute(f"""
SELECT
  -- count_10min: among non-null
  AVG(CASE WHEN count_10min = 0 THEN 1 ELSE 0 END) FILTER (WHERE count_10min IS NOT NULL) AS count_zero_among_present,
  AVG(CASE WHEN count_10min > 0 THEN 1 ELSE 0 END) FILTER (WHERE count_10min IS NOT NULL) AS count_pos_among_present,

  -- occupancy_time_10min: among non-null
  AVG(CASE WHEN occupancy_time_10min = 0 THEN 1 ELSE 0 END) FILTER (WHERE occupancy_time_10min IS NOT NULL) AS occ_zero_among_present,
  AVG(CASE WHEN occupancy_time_10min > 0 THEN 1 ELSE 0 END) FILTER (WHERE occupancy_time_10min IS NOT NULL) AS occ_pos_among_present
FROM '{P}';
""").fetchone()


# How many rows have both count and occupancy_time missing, vs at least one present?
con.execute(f"""
SELECT
  AVG(CASE WHEN count_10min IS NULL AND occupancy_time_10min IS NULL THEN 1 ELSE 0 END) AS both_nan_rate,
  AVG(CASE WHEN count_10min IS NOT NULL OR  occupancy_time_10min IS NOT NULL THEN 1 ELSE 0 END) AS any_present_rate
FROM '{P}';
""").fetchone()
