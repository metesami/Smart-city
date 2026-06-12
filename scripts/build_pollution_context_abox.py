import argparse

from smartcity.kg.pollution_context_abox import build_pollution_context_abox
from smartcity.utils.logging import setup_logger


def main():
    parser = argparse.ArgumentParser(description="Build network-level pollution context ABox.")

    parser.add_argument("--pollution-csv", required=True, help="Processed pollution CSV.")
    parser.add_argument("--output-ttl", required=True, help="Output pollution context ABox TTL.")

    args = parser.parse_args()

    logger = setup_logger(
        name="pollution_context_abox",
        log_file="outputs/logs/pollution_context_abox.log",
    )

    logger.info("Starting pollution context ABox generation")
    logger.info(f"Pollution CSV: {args.pollution_csv}")
    logger.info(f"Output TTL: {args.output_ttl}")

    build_pollution_context_abox(
        pollution_csv=args.pollution_csv,
        output_ttl=args.output_ttl,
    )

    logger.info("Pollution context ABox generation finished successfully")


if __name__ == "__main__":
    main()