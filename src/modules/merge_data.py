import pandas as pd
from glob import glob

files = glob(r"C:\Users\fcpen\Documents\Trains_project\Service_data_csv\location_gb-nr_RDNGSTN_*.csv") #returns a list of files following that pattern
print(f"Found {len(files)} files")

dfs = []
for f in files:
    temp_df = pd.read_csv(f, skiprows=2) #ensures the real header is used
    dfs.append(temp_df)
    # print(temp_df.columns.tolist())
    # print(temp_df.head())

df = pd.concat(dfs, ignore_index=True)

df["run_date"] = pd.to_datetime(df["run_date_clean"], dayfirst=True, errors='coerce', infer_datetime_format=True)

df.tail(10)

# df["gbtt_dep"] = pd.to_datetime(df["gbtt_dep"], errors='coerce')
# df["gbtt_arr"] = pd.to_datetime(df["gbtt_arr"], errors='coerce')
# df = df.sort_values(["run_date", "gbtt_dep", "gbtt_arr"])

# print(df.columns.tolist())
# print(len(df.columns.tolist()))

# df.to_csv(r"C:\Users\fcpen\Documents\GitHub\Train_delays_and_services\data\RDG_2024-2025_ALL.csv", index=False, date_format="%d/%m/%Y")