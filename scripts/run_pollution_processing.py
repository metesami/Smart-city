import argparse

from smartcity.pollution.processing import combine_pollution
from smartcity.utils.logging import setup_logger


def parse_station_file_args(values: list[str]) -> dict[str, str]:
    station_files = {}

    for value in values:
        if "=" not in value:
            raise ValueError(
                "Station files must be passed as StationID=path, "
                "for example: DEHE001=data/raw/pollution/DEHE001.txt"
            )

        station_id, path = value.split("=", 1)
        station_files[station_id.strip()] = path.strip()

    return station_files


def main():
    parser = argparse.ArgumentParser(
        description="Combine raw pollution station files into station-level pollution dataset."
    )

    parser.add_argument(
        "--station-file",
        action="append",
        required=True,
        help="Station file mapping as StationID=path. Can be repeated.",
    )
    parser.add_argument("--output", required=True, help="Output pollution CSV.")
    parser.add_argument(
        "--freq",
        default="10min",
        help="Output frequency, e.g. 10min, 15min, 30min, 1h.",
    )
    parser.add_argument(
        "--fill-limit",
        type=int,
        default=2,
        help="Forward-fill limit after resampling. Use 0 to disable.",
    )

    args = parser.parse_args()

    logger = setup_logger(
        name="pollution_processing",
        log_file="outputs/logs/pollution_processing.log",
    )

    station_files = parse_station_file_args(args.station_file)

    logger.info("Starting pollution processing pipeline")
    logger.info(f"Stations: {station_files}")
    logger.info(f"Output: {args.output}")
    logger.info(f"Frequency: {args.freq}")
    logger.info(f"Fill limit: {args.fill_limit}")

    fill_limit = args.fill_limit if args.fill_limit > 0 else None

    combine_pollution(
        station_files=station_files,
        output_path=args.output,
        freq=args.freq,
        fill_limit=fill_limit,
    )

    logger.info("Pollution processing pipeline finished successfully")


if __name__ == "__main__":
    main()