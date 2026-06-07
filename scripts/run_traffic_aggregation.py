import argparse

from smartcity.traffic.aggregation import run_traffic_aggregation
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate imputed traffic data to a selected temporal granularity."
    )

    parser.add_argument("--input", required=True, help="Input imputed traffic parquet file.")
    parser.add_argument("--output", required=True, help="Output aggregated parquet file.")
    parser.add_argument(
        "--freq",
        default="10min",
        help="Aggregation frequency, e.g. 10min, 30min, 1h.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=8,
        help="Number of DuckDB threads.",
    )

    args = parser.parse_args()

    logger = setup_logger(
        name="traffic_aggregation",
        log_file="outputs/logs/traffic_aggregation.log",
    )

    logger.info("Starting traffic aggregation pipeline")
    logger.info(f"Input: {args.input}")
    logger.info(f"Output: {args.output}")
    logger.info(f"Frequency: {args.freq}")
    logger.info(f"Threads: {args.threads}")

    run_traffic_aggregation(
        input_path=args.input,
        output_path=args.output,
        freq=args.freq,
        threads=args.threads,
    )

    logger.info("Traffic aggregation pipeline finished successfully")


if __name__ == "__main__":
    main()