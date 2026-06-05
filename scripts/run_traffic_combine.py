import argparse

from smartcity.traffic.combine import combine_traffic_files
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(
        description="Combine raw traffic CSV files for one intersection."
    )

    parser.add_argument("--traffic-root", required=True, help="Root folder containing raw traffic CSV files.")
    parser.add_argument("--metadata-file", required=True, help="Intersection metadata file containing sensor_id column.")
    parser.add_argument("--intersection-id", required=True, help="Intersection ID, e.g. A142.")
    parser.add_argument("--output", required=True, help="Output combined 1-minute CSV file.")
    parser.add_argument("--sensor-column", default="sensor_id", help="Sensor ID column in metadata file.")

    args = parser.parse_args()

    logger = setup_logger(
        name="traffic_combine",
        log_file=f"outputs/logs/{args.intersection_id}_traffic_combine.log",
    )

    logger.info("Starting traffic combine pipeline")
    logger.info(f"Traffic root: {args.traffic_root}")
    logger.info(f"Metadata file: {args.metadata_file}")
    logger.info(f"Intersection ID: {args.intersection_id}")
    logger.info(f"Output: {args.output}")

    combine_traffic_files(
        traffic_root=args.traffic_root,
        metadata_file=args.metadata_file,
        intersection_id=args.intersection_id,
        output_path=args.output,
        sensor_column=args.sensor_column,
    )

    logger.info("Traffic combine pipeline finished successfully")


if __name__ == "__main__":
    main()