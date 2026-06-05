import argparse

from smartcity.traffic.cleaning import process_file


def main():
    parser = argparse.ArgumentParser(
        description="Run traffic cleaning pipeline for one intersection."
    )

    parser.add_argument("--input", required=True, help="Path to raw traffic file")
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument("--intersection-id", required=True, help="Intersection ID, e.g. A142")

    args = parser.parse_args()

    process_file(
        input_path=args.input,
        outdir=args.outdir,
        intersection_id=args.intersection_id,
    )


if __name__ == "__main__":
    main()