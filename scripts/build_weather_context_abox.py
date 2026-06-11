import argparse

from smartcity.kg.weather_context_abox import build_weather_context_abox
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(description="Build city-wide weather context ABox.")

    parser.add_argument("--weather-csv", required=True, help="Processed weather CSV.")
    parser.add_argument("--output-ttl", required=True, help="Output weather context ABox TTL.")

    args = parser.parse_args()

    logger = setup_logger(
        name="weather_context_abox",
        log_file="outputs/logs/weather_context_abox.log",
    )

    logger.info("Starting weather context ABox generation")
    logger.info(f"Weather CSV: {args.weather_csv}")
    logger.info(f"Output TTL: {args.output_ttl}")

    build_weather_context_abox(
        weather_csv=args.weather_csv,
        output_ttl=args.output_ttl,
    )

    logger.info("Weather context ABox generation finished successfully")


if __name__ == "__main__":
    main()