import argparse

from smartcity.pollution.categorization import categorize_pollution
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(
        description="Add pollution category bins and timestamp_seconds to processed pollution data."
    )

    parser.add_argument("--input", required=True, help="Input processed pollution CSV.")
    parser.add_argument("--output", required=True, help="Output categorized pollution CSV.")

    args = parser.parse_args()

    logger = setup_logger(
        name="pollution_categorization",
        log_file="outputs/logs/pollution_categorization.log",
    )

    logger.info("Starting pollution categorization pipeline")
    logger.info(f"Input: {args.input}")
    logger.info(f"Output: {args.output}")

    categorize_pollution(
        input_path=args.input,
        output_path=args.output,
    )

    logger.info("Pollution categorization pipeline finished successfully")


if __name__ == "__main__":
    main()