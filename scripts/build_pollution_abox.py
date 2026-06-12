import argparse

from smartcity.kg.pollution_abox import build_pollution_abox
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(description="Build pollution observation ABox.")

    parser.add_argument("--metadata", required=True, help="Pollution station metadata CSV.")
    parser.add_argument("--pollution-csv", required=True, help="Processed pollution CSV.")
    parser.add_argument("--output-ttl", required=True, help="Output pollution ABox TTL.")
    parser.add_argument("--chunk-size", type=int, default=2000)

    args = parser.parse_args()

    logger = setup_logger(
        name="pollution_abox",
        log_file="outputs/logs/pollution_abox.log",
    )

    logger.info("Starting pollution ABox generation")
    logger.info(f"Metadata: {args.metadata}")
    logger.info(f"Pollution CSV: {args.pollution_csv}")
    logger.info(f"Output TTL: {args.output_ttl}")

    build_pollution_abox(
        metadata_path=args.metadata,
        pollution_csv=args.pollution_csv,
        output_ttl=args.output_ttl,
        chunk_size=args.chunk_size,
    )

    logger.info("Pollution ABox generation finished successfully")


if __name__ == "__main__":
    main()