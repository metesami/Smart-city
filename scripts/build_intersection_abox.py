import argparse

from smartcity.kg.intersection_abox import build_intersection_abox
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(description="Build intersection metadata ABox.")

    parser.add_argument("--metadata-file", required=True)
    parser.add_argument("--intersection-id", required=True)
    parser.add_argument("--output-ttl", required=True)
    parser.add_argument("--sensor-map-json", required=True)
    parser.add_argument("--lane-map-json", required=True)
    parser.add_argument("--sensor-to-lane-json", required=True)
    parser.add_argument("--no-fetch-osm", action="store_true")

    args = parser.parse_args()

    logger = setup_logger(
        name="intersection_abox",
        log_file=f"outputs/logs/{args.intersection_id}_intersection_abox.log",
    )

    logger.info("Starting intersection ABox generation")

    build_intersection_abox(
        metadata_file=args.metadata_file,
        intersection_id=args.intersection_id,
        output_ttl=args.output_ttl,
        sensor_map_json=args.sensor_map_json,
        lane_map_json=args.lane_map_json,
        sensor_to_lane_json=args.sensor_to_lane_json,
        fetch_osm=not args.no_fetch_osm,
    )

    logger.info("Intersection ABox generation finished")


if __name__ == "__main__":
    main()