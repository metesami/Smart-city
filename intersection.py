from pathlib import Path
import pandas as pd

# folders
input_dir = Path("C:\PhD\Dataset\Traffic\Intersections")
csv_dir = Path("data/intersections/csv")

csv_dir.mkdir(parents=True, exist_ok=True)

all_dfs = []

# loop over excel files
for xlsx_file in input_dir.glob("*.xlsx"):

    print(f"Processing: {xlsx_file.name}")

    # read excel
    df = pd.read_excel(xlsx_file)

    # save csv
    csv_file = csv_dir / f"{xlsx_file.stem}.csv"
    df.to_csv(csv_file, index=False)

    # add source file name
    df["source_file"] = xlsx_file.name

    all_dfs.append(df)

# create master dataset
master_df = pd.concat(all_dfs, ignore_index=True)

master_df.to_csv(
    "data/intersections/darmstadt_intersections_all.csv",
    index=False
)

print("Done!")