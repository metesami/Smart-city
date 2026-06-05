import argparse

from smartcity.imputation.traffic_imputation import run_traffic_imputation
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(
        description="Run traffic imputation pipeline."
    )

    parser.add_argument("--input", required=True, help="Cleaned traffic input file.")
    parser.add_argument("--output", required=True, help="Imputed traffic output parquet.")

    args = parser.parse_args()

    logger = setup_logger(
        name="traffic_imputation",
        log_file="outputs/logs/traffic_imputation.log",
    )

    logger.info("Starting traffic imputation pipeline")
    logger.info(f"Input: {args.input}")
    logger.info(f"Output: {args.output}")

    run_traffic_imputation(
        input_path=args.input,
        output_path=args.output,
    )

    logger.info("Traffic imputation pipeline finished successfully")


if __name__ == "__main__":
    main()