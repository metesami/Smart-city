import argparse
from smartcity.utils.logging import setup_logger
from smartcity.traffic.cleaning import process_file


def main():
    parser = argparse.ArgumentParser(
        description="Run traffic cleaning pipeline for one intersection."
    )

    parser.add_argument("--input", required=True, help="Path to raw traffic file")
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument("--intersection-id", required=True, help="Intersection ID, e.g. A142")

    args = parser.parse_args()
    logger = setup_logger(
        name="traffic_cleaning",
        log_file=f"outputs/logs/{args.intersection_id}_traffic_cleaning.log",
    )

    logger.info("Starting traffic cleaning pipeline")
    logger.info(f"Input file: {args.input}")
    logger.info(f"Output directory: {args.outdir}")
    logger.info(f"Intersection ID: {args.intersection_id}")
    process_file(
        input_path=args.input,
        outdir=args.outdir,
        intersection_id=args.intersection_id,
    )
    logger.info("Traffic cleaning pipeline finished successfully")

if __name__ == "__main__":
    main()