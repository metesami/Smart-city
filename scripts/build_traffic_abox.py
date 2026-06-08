import argparse

from smartcity.kg.traffic_abox import build_traffic_abox
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(description="Build traffic observation ABox.")

    parser.add_argument("--input-parquet", required=True)
    parser.add_argument("--output-nt-gz", required=True)
    parser.add_argument("--sensor-map-json", required=True)
    parser.add_argument("--sensor-to-lane-json", required=True)
    parser.add_argument("--batch-size", type=int, default=100000)
    parser.add_argument("--threads", type=int, default=8)

    args = parser.parse_args()

    logger = setup_logger(
        name="traffic_abox",
        log_file="outputs/logs/traffic_abox.log",
    )

    logger.info("Starting traffic ABox generation")

    build_traffic_abox(
        input_parquet=args.input_parquet,
        output_nt_gz=args.output_nt_gz,
        sensor_map_json=args.sensor_map_json,
        sensor_to_lane_json=args.sensor_to_lane_json,
        batch_size=args.batch_size,
        threads=args.threads,
    )

    logger.info("Traffic ABox generation finished")


if __name__ == "__main__":
    main()