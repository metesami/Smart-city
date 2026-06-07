import argparse
from pathlib import Path

import pandas as pd

from smartcity.utils.logging import setup_logger


def load_table(path: str | Path) -> pd.DataFrame:
    path = Path(path)

    if path.suffix == ".parquet":
        return pd.read_parquet(path)

    if path.suffix == ".csv":
        return pd.read_csv(path)

    if path.suffixes[-2:] == [".csv", ".gz"]:
        return pd.read_csv(path, compression="gzip")

    raise ValueError(f"Unsupported file format: {path}")


def summarize_imputation(df: pd.DataFrame) -> pd.DataFrame:
    required = {
        "sensor_id",
        "missing_reason",
        "impute_method",
        "count_clean",
        "dwell_clean",
        "count_imputed",
        "dwell_imputed",
    }

    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    rows = []

    for sensor_id, g in df.groupby("sensor_id"):
        total = len(g)

        clean_available = (
            g["count_clean"].notna() & g["dwell_clean"].notna()
        ).sum()

        imputed_available = (
            g["count_imputed"].notna() & g["dwell_imputed"].notna()
        ).sum()

        newly_imputed = (
            g["count_clean"].isna()
            & g["count_imputed"].notna()
            & g["dwell_imputed"].notna()
        ).sum()

        still_missing = (
            g["count_imputed"].isna() | g["dwell_imputed"].isna()
        ).sum()

        rows.append(
            {
                "sensor_id": sensor_id,
                "minutes_total": total,
                "clean_available": int(clean_available),
                "imputed_available": int(imputed_available),
                "newly_imputed": int(newly_imputed),
                "still_missing_after_imputation": int(still_missing),
                "clean_available_rate": clean_available / total if total else 0,
                "imputed_available_rate": imputed_available / total if total else 0,
                "newly_imputed_rate": newly_imputed / total if total else 0,
                "still_missing_rate": still_missing / total if total else 0,
                "temporal_linear_count": int((g["impute_method"] == "TEMPORAL_LINEAR").sum()),
                "rolling_median_count": int((g["impute_method"] == "ROLLING_MEDIAN").sum()),
                "profile_median_count": int((g["impute_method"] == "PROFILE_MEDIAN").sum()),
                "logic_invalid_count": int((g["missing_reason"] == "LOGIC_INVALID").sum()),
                "stuck_off_count": int((g["missing_reason"] == "STUCK_OFF").sum()),
                "cap_exceeded_count": int((g["missing_reason"] == "CAP_EXCEEDED").sum()),
                "profile_hard_count": int((g["missing_reason"] == "PROFILE_HARD").sum()),
            }
        )

    return pd.DataFrame(rows).sort_values("still_missing_rate", ascending=False)


def main():
    parser = argparse.ArgumentParser(
        description="Summarize traffic imputation results per sensor."
    )

    parser.add_argument("--input", required=True, help="Path to imputed traffic file.")
    parser.add_argument("--output", required=True, help="Path to output summary CSV.")

    args = parser.parse_args()

    logger = setup_logger(
        name="traffic_imputation_summary",
        log_file="outputs/logs/traffic_imputation_summary.log",
    )

    logger.info("Loading imputed traffic data")
    df = load_table(args.input)

    logger.info("Building imputation summary")
    summary = summarize_imputation(df)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output_path, index=False)

    logger.info(f"Saved imputation summary: {output_path}")

    print(summary.head(20))
    print(f"\nSaved summary to: {output_path}")


if __name__ == "__main__":
    main()