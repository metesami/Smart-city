from pathlib import Path

import duckdb


def validate_traffic_aggregation(
    input_path: str | Path,
    summary_output: str | Path,
    sensor_output: str | Path,
    expected_freq: str = "10min",
    threads: int = 8,
) -> tuple[Path, Path]:
    input_path = Path(input_path)
    summary_output = Path(summary_output)
    sensor_output = Path(sensor_output)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    summary_output.parent.mkdir(parents=True, exist_ok=True)
    sensor_output.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={threads};")
    con.execute("SET preserve_insertion_order=false;")

    input_sql = input_path.as_posix()
    summary_sql = summary_output.as_posix()
    sensor_sql = sensor_output.as_posix()

    summary_query = f"""
    COPY (
        SELECT
            COUNT(*) AS rows_total,
            COUNT(DISTINCT sensor_id) AS sensor_count,

            MIN(timestamp) AS start_timestamp,
            MAX(timestamp) AS end_timestamp,

            AVG(coverage_count) AS coverage_count_mean,
            MIN(coverage_count) AS coverage_count_min,
            AVG(coverage_dwell) AS coverage_dwell_mean,
            MIN(coverage_dwell) AS coverage_dwell_min,

            AVG(imputed_rate) AS imputed_rate_mean,
            MAX(imputed_rate) AS imputed_rate_max,

            AVG(CASE WHEN count_agg IS NULL THEN 1 ELSE 0 END) AS count_null_rate,
            AVG(CASE WHEN occupancy_time_agg IS NULL THEN 1 ELSE 0 END) AS occupancy_null_rate,

            MIN(count_agg) AS count_min,
            MAX(count_agg) AS count_max,
            AVG(count_agg) AS count_mean,

            MIN(occupancy_time_agg) AS occupancy_time_min,
            MAX(occupancy_time_agg) AS occupancy_time_max,
            AVG(occupancy_time_agg) AS occupancy_time_mean,

            MIN(occupancy_ratio) AS occupancy_ratio_min,
            MAX(occupancy_ratio) AS occupancy_ratio_max,
            AVG(occupancy_ratio) AS occupancy_ratio_mean,

            AVG(soft_flag_rate) AS soft_flag_rate_mean,
            AVG(profile_flag_hard_rate) AS profile_flag_hard_rate_mean,
            AVG(spike_flag_rate) AS spike_flag_rate_mean,

            AVG(CASE WHEN freq = '{expected_freq}' THEN 1 ELSE 0 END) AS expected_freq_rate

        FROM read_parquet('{input_sql}')
    ) TO '{summary_sql}' (HEADER, DELIMITER ',');
    """

    sensor_query = f"""
    COPY (
        SELECT
            sensor_id,

            COUNT(*) AS rows_total,
            MIN(timestamp) AS start_timestamp,
            MAX(timestamp) AS end_timestamp,

            AVG(coverage_count) AS coverage_count_mean,
            MIN(coverage_count) AS coverage_count_min,
            AVG(coverage_dwell) AS coverage_dwell_mean,
            MIN(coverage_dwell) AS coverage_dwell_min,

            AVG(imputed_rate) AS imputed_rate_mean,
            MAX(imputed_rate) AS imputed_rate_max,

            AVG(CASE WHEN count_agg IS NULL THEN 1 ELSE 0 END) AS count_null_rate,
            AVG(CASE WHEN occupancy_time_agg IS NULL THEN 1 ELSE 0 END) AS occupancy_null_rate,

            MIN(count_agg) AS count_min,
            MAX(count_agg) AS count_max,
            AVG(count_agg) AS count_mean,

            MIN(occupancy_ratio) AS occupancy_ratio_min,
            MAX(occupancy_ratio) AS occupancy_ratio_max,
            AVG(occupancy_ratio) AS occupancy_ratio_mean,

            AVG(soft_flag_rate) AS soft_flag_rate_mean,
            AVG(profile_flag_hard_rate) AS profile_flag_hard_rate_mean,
            AVG(spike_flag_rate) AS spike_flag_rate_mean,

            AVG(CASE WHEN freq = '{expected_freq}' THEN 1 ELSE 0 END) AS expected_freq_rate

        FROM read_parquet('{input_sql}')
        GROUP BY sensor_id
        ORDER BY sensor_id
    ) TO '{sensor_sql}' (HEADER, DELIMITER ',');
    """

    con.execute(summary_query)
    con.execute(sensor_query)
    con.close()

    print("Traffic aggregation validation finished.")
    print(f"Input: {input_path}")
    print(f"Summary output: {summary_output}")
    print(f"Sensor output: {sensor_output}")

    return summary_output, sensor_output