import argparse

from smartcity.traffic.aggregation_validation import validate_traffic_aggregation
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(
        description="Validate aggregated traffic parquet output."
    )

    parser.add_argument("--input", required=True, help="Aggregated traffic parquet file.")
    parser.add_argument("--summary-output", required=True, help="Dataset-level validation CSV.")
    parser.add_argument("--sensor-output", required=True, help="Per-sensor validation CSV.")
    parser.add_argument("--expected-freq", default="10min", help="Expected frequency label.")
    parser.add_argument("--threads", type=int, default=8, help="DuckDB thread count.")

    args = parser.parse_args()

    logger = setup_logger(
        name="traffic_aggregation_validation",
        log_file="outputs/logs/traffic_aggregation_validation.log",
    )

    logger.info("Starting traffic aggregation validation")
    logger.info(f"Input: {args.input}")
    logger.info(f"Summary output: {args.summary_output}")
    logger.info(f"Sensor output: {args.sensor_output}")
    logger.info(f"Expected frequency: {args.expected_freq}")

    validate_traffic_aggregation(
        input_path=args.input,
        summary_output=args.summary_output,
        sensor_output=args.sensor_output,
        expected_freq=args.expected_freq,
        threads=args.threads,
    )

    logger.info("Traffic aggregation validation finished successfully")


if __name__ == "__main__":
    main()