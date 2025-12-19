import pandas as pd
from glob import glob

files = glob(r"C:\Users\fcpen\Documents\Trains_project\Service_data_csv\location_gb-nr_RDNGSTN_*.csv") #returns a list of files following that pattern
print(f"Found {len(files)} files")

dfs = []
for f in files:
    dfs.append(pd.read_csv(f, skiprows=2)) #ensures the real header is used

df = pd.concat(dfs, ignore_index=True)

df["run_date"] = pd.to_datetime(df["run_date"], dayfirst=True, errors='coerce')
invalid_dates = df[df["run_date"].isna()]
if not invalid_dates.empty:
    print("Rows with invalid dates:")
    print(invalid_dates)

# 3. Convert back to string in DD/MM/YYYY format
df["run_date"] = df["run_date"].dt.strftime("%d/%m/%Y")

df["gbtt_dep"] = pd.to_datetime(df["gbtt_dep"], errors='coerce')
df["gbtt_arr"] = pd.to_datetime(df["gbtt_arr"], errors='coerce')
df = df.sort_values(["run_date", "gbtt_dep", "gbtt_arr"])

print(df.columns.tolist())
print(len(df.columns.tolist()))

df.to_csv(r"C:\Users\fcpen\Documents\GitHub\Train_delays_and_services\data\RDG_2024-2025_ALL.csv", index=False)