from pathlib import Path

import duckdb


def frequency_to_seconds(freq: str) -> int:
    freq = freq.lower().strip()

    if freq.endswith("min"):
        return int(freq.replace("min", "")) * 60

    if freq.endswith("h"):
        return int(freq.replace("h", "")) * 3600

    raise ValueError(
        f"Unsupported frequency: {freq}. Use examples like '10min', '30min', or '1h'."
    )


def frequency_to_label(freq: str) -> str:
    return freq.lower().strip().replace("min", "min").replace("h", "h")


def run_traffic_aggregation(
    input_path: str | Path,
    output_path: str | Path,
    freq: str = "10min",
    threads: int = 8,
) -> Path:
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    freq_seconds = frequency_to_seconds(freq)
    freq_ms = freq_seconds * 1000
    freq_label = frequency_to_label(freq)

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={threads};")

    query = f"""
    COPY (
      WITH base AS (
        SELECT
          sensor_id,

          to_timestamp(
            FLOOR(EXTRACT(EPOCH FROM CAST(timestamp AS TIMESTAMPTZ)) / {freq_seconds}) * {freq_seconds}
          ) AS timestamp,

          TRY_CAST(count_imputed AS DOUBLE) AS count_imputed,
          TRY_CAST(dwell_imputed AS DOUBLE) AS dwell_imputed,

          CASE WHEN COALESCE(soft_flag, 0) IN (1, TRUE) THEN 1 ELSE 0 END AS soft_flag,
          CASE WHEN COALESCE(profile_flag_hard, 0) IN (1, TRUE) THEN 1 ELSE 0 END AS profile_flag_hard,
          CASE WHEN COALESCE(spike_flag, 0) IN (1, TRUE) THEN 1 ELSE 0 END AS spike_flag,

          CASE WHEN COALESCE(is_clean_observed, 0) IN (1, TRUE) THEN 1 ELSE 0 END AS is_clean_observed,
          CASE WHEN COALESCE(imputable, 0) IN (1, TRUE) THEN 1 ELSE 0 END AS imputable,

          NULLIF(TRIM(CAST(impute_method AS VARCHAR)), '') AS impute_method,
          NULLIF(TRIM(CAST(missing_reason AS VARCHAR)), '') AS missing_reason

        FROM read_parquet('{input_path.as_posix()}')
        WHERE timestamp IS NOT NULL AND sensor_id IS NOT NULL
      ),

      agg AS (
        SELECT
          sensor_id,
          timestamp,

          AVG(CASE WHEN count_imputed IS NULL THEN 0 ELSE 1 END) AS coverage_count,
          AVG(CASE WHEN dwell_imputed IS NULL THEN 0 ELSE 1 END) AS coverage_dwell,

          SUM(count_imputed) FILTER (WHERE count_imputed IS NOT NULL) AS count_agg,

          SUM(dwell_imputed) FILTER (WHERE dwell_imputed IS NOT NULL) AS occupancy_time_agg,

          AVG(is_clean_observed) AS is_clean_observed_rate,
          AVG(imputable) AS imputable_rate,

          MAX(soft_flag) AS soft_flag_any,
          AVG(soft_flag) AS soft_flag_rate,

          MAX(profile_flag_hard) AS profile_flag_hard_any,
          AVG(profile_flag_hard) AS profile_flag_hard_rate,

          MAX(spike_flag) AS spike_flag_any,
          AVG(spike_flag) AS spike_flag_rate,

          MODE(impute_method) AS impute_method_mode,
          COUNT(DISTINCT impute_method) FILTER (WHERE impute_method IS NOT NULL)
            AS impute_method_nunique,

          MODE(missing_reason) AS missing_reason_mode,
          COUNT(DISTINCT missing_reason) FILTER (WHERE missing_reason IS NOT NULL)
            AS missing_reason_nunique,

          COUNT(*) AS minute_rows,
          SUM(CASE WHEN count_imputed IS NOT NULL THEN 1 ELSE 0 END) AS available_count_minutes,
          SUM(CASE WHEN dwell_imputed IS NOT NULL THEN 1 ELSE 0 END) AS available_dwell_minutes,
          SUM(CASE WHEN impute_method IS NOT NULL AND impute_method <> 'NONE' THEN 1 ELSE 0 END)
            AS imputed_minutes,
          SUM(CASE WHEN count_imputed IS NULL OR dwell_imputed IS NULL THEN 1 ELSE 0 END)
            AS missing_minutes

        FROM base
        GROUP BY sensor_id, timestamp
      )

      SELECT
        sensor_id,
        timestamp,

        coverage_count,
        coverage_dwell,

        count_agg,
        occupancy_time_agg,

        CASE
          WHEN count_agg IS NOT NULL AND count_agg > 0
          THEN occupancy_time_agg / count_agg
          ELSE NULL
        END AS avg_dwell_agg,

        is_clean_observed_rate,
        imputable_rate,

        soft_flag_any,
        soft_flag_rate,

        profile_flag_hard_any,
        profile_flag_hard_rate,

        spike_flag_any,
        spike_flag_rate,

        impute_method_mode,
        impute_method_nunique,

        missing_reason_mode,
        missing_reason_nunique,

        minute_rows,
        available_count_minutes,
        available_dwell_minutes,
        imputed_minutes,
        missing_minutes,

        CASE
          WHEN minute_rows > 0
          THEN imputed_minutes::DOUBLE / minute_rows
          ELSE NULL
        END AS imputed_rate,

        CASE
          WHEN occupancy_time_agg IS NOT NULL
          THEN occupancy_time_agg / {freq_ms}.0
          ELSE NULL
        END AS occupancy_ratio,

        EXTRACT(dow FROM timestamp) AS weekday,
        (EXTRACT(hour FROM timestamp) * 60 + EXTRACT(minute FROM timestamp))::INTEGER AS minute,

        '{freq_label}' AS freq

      FROM agg
      ORDER BY sensor_id, timestamp
    ) TO '{output_path.as_posix()}' (FORMAT PARQUET);
    """

    con.execute(query)
    con.close()

    print("Traffic aggregation finished.")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"Frequency: {freq}")
    print(f"Frequency seconds: {freq_seconds}")

    return output_path