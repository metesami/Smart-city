import argparse

from smartcity.kg.weather_abox import build_weather_abox
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(description="Build weather observation ABox.")

    parser.add_argument("--metadata", required=True, help="Weather station metadata CSV.")
    parser.add_argument("--weather-csv", required=True, help="Processed weather CSV.")
    parser.add_argument("--output-ttl", required=True, help="Output weather ABox TTL.")
    parser.add_argument("--chunk-size", type=int, default=5000)

    args = parser.parse_args()

    logger = setup_logger(
        name="weather_abox",
        log_file="outputs/logs/weather_abox.log",
    )

    logger.info("Starting weather ABox generation")
    logger.info(f"Metadata: {args.metadata}")
    logger.info(f"Weather CSV: {args.weather_csv}")
    logger.info(f"Output TTL: {args.output_ttl}")

    build_weather_abox(
        metadata_path=args.metadata,
        weather_csv=args.weather_csv,
        output_ttl=args.output_ttl,
        chunk_size=args.chunk_size,
    )

    logger.info("Weather ABox generation finished successfully")


if __name__ == "__main__":
    main()