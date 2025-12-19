import pandas as pd
from glob import glob

files = glob(r"C:\Users\fcpen\Documents\Trains_project\Service_data_csv\location_gb-nr_RDNGSTN_*.csv") #returns a list of files following that pattern
print(f"Found {len(files)} files")

dfs = []
for f in files[20:25]:
    temp_df = pd.read_csv(f, skiprows=2, header=0) #ensures the real header is used
    dfs.append(temp_df)
    print(f, temp_df.columns.tolist())

# df = pd.concat(dfs, ignore_index=True)

# df.tail(10)

# df["run_date_raw"] = df["run_date"]

# df["run_date_clean"] = df["run_date_raw"].astype(str).str.strip().str.replace(r'[\u200b\u200c\u200d\u00A0\u200e]', '', regex=True) #stripping any leading/trailing spaces and other odd characters
# df["run_date_parsed"] = pd.to_datetime(df["run_date_clean"], dayfirst=True, errors='coerce', infer_datetime_format=True)

# print(df[df["run_date_parsed"].isna()][["run_date_raw"]])

# # 3. Convert back to string in DD/MM/YYYY format
# df["run_date"] = df["run_date"].dt.strftime("%d/%m/%Y")

# df["gbtt_dep"] = pd.to_datetime(df["gbtt_dep"], errors='coerce')
# df["gbtt_arr"] = pd.to_datetime(df["gbtt_arr"], errors='coerce')
# df = df.sort_values(["run_date", "gbtt_dep", "gbtt_arr"])

# print(df.columns.tolist())
# print(len(df.columns.tolist()))

# df.to_csv(r"C:\Users\fcpen\Documents\GitHub\Train_delays_and_services\data\RDG_2024-2025_ALL.csv", index=False)