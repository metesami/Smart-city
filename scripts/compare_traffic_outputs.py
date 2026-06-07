import argparse
from pathlib import Path

import pandas as pd


def load_table(path: str | Path) -> pd.DataFrame:
    path = Path(path)

    if path.suffix == ".parquet":
        return pd.read_parquet(path)

    if path.suffix == ".csv":
        return pd.read_csv(path)

    if path.suffixes[-2:] == [".csv", ".gz"]:
        return pd.read_csv(path, compression="gzip")

    raise ValueError(f"Unsupported file format: {path}")


def main():
    parser = argparse.ArgumentParser(
        description="Compare legacy and new traffic cleaning outputs."
    )

    parser.add_argument("--old", required=True, help="Path to old legacy output file")
    parser.add_argument("--new", required=True, help="Path to new pipeline output file")

    args = parser.parse_args()

    old_df = load_table(args.old)
    new_df = load_table(args.new)

    print("Old shape:", old_df.shape)
    print("New shape:", new_df.shape)

    if old_df.shape != new_df.shape:
        print("WARNING: Shapes are different.")

    common_cols = [c for c in old_df.columns if c in new_df.columns]
    missing_in_new = [c for c in old_df.columns if c not in new_df.columns]
    missing_in_old = [c for c in new_df.columns if c not in old_df.columns]

    print("Common columns:", len(common_cols))
    print("Missing in new:", missing_in_new)
    print("Missing in old:", missing_in_old)

    old_common = old_df[common_cols].sort_index(axis=1).reset_index(drop=True)
    new_common = new_df[common_cols].sort_index(axis=1).reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(
            old_common,
            new_common,
            check_dtype=False,
            check_exact=False,
            rtol=1e-6,
            atol=1e-6,
        )
        print("OK: Legacy and new outputs are equivalent.")
    except AssertionError as error:
        print("FAILED: Outputs are not equivalent.")
        print(str(error)[:3000])


if __name__ == "__main__":
    main()
