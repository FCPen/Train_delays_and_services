import pandas as pd
from glob import glob

files = glob(r"C:\Users\fcpen\Documents\Trains_project\Service_data_csv\location_gb-nr_RDNGSTN_*.csv") #returns a list of files following that pattern
print(f"Found {len(files)} files")

df = pd.concat(pd.read_csv(f) for f in files)

df["run_date"] = pd.to_datetime(df["run_date"])
df = df.sort_values(["run_date", "gbtt_dep", "gbtt_arr"])

df.to_csv(r"..\data\RDG_2024-2025_ALL.csv", index=False)