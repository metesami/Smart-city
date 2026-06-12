import argparse

from smartcity.weather.categorization import categorize_weather
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(
        description="Add weather category bins and timestamp_seconds to processed weather data."
    )

    parser.add_argument("--input", required=True, help="Input processed weather CSV.")
    parser.add_argument("--output", required=True, help="Output categorized weather CSV.")

    args = parser.parse_args()

    logger = setup_logger(
        name="weather_categorization",
        log_file="outputs/logs/weather_categorization.log",
    )

    logger.info("Starting weather categorization pipeline")
    logger.info(f"Input: {args.input}")
    logger.info(f"Output: {args.output}")

    categorize_weather(
        input_path=args.input,
        output_path=args.output,
    )

    logger.info("Weather categorization pipeline finished successfully")


if __name__ == "__main__":
    main()