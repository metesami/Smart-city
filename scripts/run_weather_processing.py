import argparse

from smartcity.weather.processing import combine_weather
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(
        description="Combine raw weather files into station-level weather dataset."
    )

    parser.add_argument("--metadata", required=True, help="Weather station metadata CSV.")
    parser.add_argument("--temperature", required=True, help="Raw temperature/humidity file.")
    parser.add_argument("--precipitation", required=True, help="Raw precipitation file.")
    parser.add_argument("--pressure", required=True, help="Raw pressure file.")
    parser.add_argument("--wind", required=True, help="Raw wind file.")
    parser.add_argument("--output", required=True, help="Output weather CSV.")
    parser.add_argument(
        "--freq",
        default="10min",
        help="Output frequency, e.g. 10min, 15min, 30min, 1h.",
    )

    args = parser.parse_args()

    logger = setup_logger(
        name="weather_processing",
        log_file="outputs/logs/weather_processing.log",
    )

    logger.info("Starting weather processing pipeline")
    logger.info(f"Metadata: {args.metadata}")
    logger.info(f"Temperature: {args.temperature}")
    logger.info(f"Precipitation: {args.precipitation}")
    logger.info(f"Pressure: {args.pressure}")
    logger.info(f"Wind: {args.wind}")
    logger.info(f"Output: {args.output}")
    logger.info(f"Frequency: {args.freq}")

    combine_weather(
        metadata_path=args.metadata,
        temperature_path=args.temperature,
        precipitation_path=args.precipitation,
        pressure_path=args.pressure,
        wind_path=args.wind,
        output_path=args.output,
        freq=args.freq,
    )

    logger.info("Weather processing pipeline finished successfully")


if __name__ == "__main__":
    main()