import pandas as pd

# Load both CSV files
lk_df = pd.read_csv("data/raw/regional/ags_lk_changes/landkreise_2023.csv")
ev_df = pd.read_csv("data/raw/electric_vehicles/registered_evs_by_regional_id/registered_evs_2018.csv")

# Extract unique regional_ids
lk_ids = set(lk_df['regional_id'].dropna().unique())
ev_ids = set(ev_df['regional_id'].dropna().unique())

# Find differences
only_in_lk = lk_ids - ev_ids
only_in_ev = ev_ids - lk_ids

# Print results
print("Regional IDs only in landkreise_2023:")
print(sorted(only_in_lk))

print("\nRegional IDs only in registered_evs_2024:")
print(sorted(only_in_ev))