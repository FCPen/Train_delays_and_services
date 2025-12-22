import pandas as pd
from glob import glob

files = glob(r"C:\Users\fcpen\Documents\Trains_project\Service_data_csv\location_gb-nr_RDNGSTN_*.csv") #returns a list of files following that pattern
print(f"Found {len(files)} files")

dfs = []
for f in files[12:16]:
    temp_df = pd.read_csv(f, skiprows=2) #ensures the real header is used
    dfs.append(temp_df)
    print(f"{f}: {len(temp_df)} rows")
    print(temp_df.columns.tolist())
    print(temp_df["run_date"].head())
    print(temp_df.iloc[:5, :5])  # first 5 rows, first 5 columns
    # print(temp_df.columns.tolist())
    # print(temp_df.head())

df = pd.concat(dfs, ignore_index=True)
# print(f"Total rows after concat: {len(df)}")

def parse_run_date(s):
    s = str(s).strip()
    try:
        return pd.to_datetime(s, format="%Y-%m-%d")
    except ValueError:
        try:
            return pd.to_datetime(s, format="%d/%m/%Y")
        except ValueError:
            return pd.NaT
 # this function should help resolve date parsing issues
df['run_date_raw'] = df['run_date']
df["run_date"] = df["run_date"].apply(parse_run_date)


# print(df.tail(10))

invalid_dates = df[df["run_date"].isna()]
print(f"Rows with invalid run_date: {len(invalid_dates)}")
print(invalid_dates.head())

bad_date = df.loc[df['run_date'].isna(), 'run_date_raw'].iloc[0]

# Show the repr to reveal hidden characters
print(repr(bad_date))

# Show Unicode codes for each character
print([hex(ord(c)) for c in bad_date])
# df["gbtt_dep"] = pd.to_datetime(df["gbtt_dep"], errors='coerce')
# df["gbtt_arr"] = pd.to_datetime(df["gbtt_arr"], errors='coerce')
# df = df.sort_values(["run_date", "gbtt_dep", "gbtt_arr"])

# print(df.columns.tolist())
# print(len(df.columns.tolist()))

# df.to_csv(r"C:\Users\fcpen\Documents\GitHub\Train_delays_and_services\data\RDG_2024-2025_ALL.csv", index=False, date_format="%d/%m/%Y")