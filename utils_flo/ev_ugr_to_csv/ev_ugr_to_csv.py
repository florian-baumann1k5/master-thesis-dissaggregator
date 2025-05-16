import pandas as pd

# read the Excel workbook + the specific sheet
df = pd.read_excel(
    "utils_flo/ev_ugr_to_csv/statistischer-bericht-verkehr-umwelt-5859007227005.xlsx",
    sheet_name="csv-85521-15"
)

# optional: turn the “Wert” column into a real number
# (the source uses German “1.234,56” formatting)
df["Wert"] = (
    df["Wert"]
      .astype(str)               # be safe
      .str.replace(",", ".", regex=False) # switch decimal comma → point
      .astype(float)
)

# write to CSV – use semicolon + German decimals if you prefer
df.to_csv(
    "utils_flo/ev_ugr_to_csv/statistischer-bericht-verkehr-umwelt-5859007227005.csv",
    index=False,      # drop the implicit row index
    sep=";",          # so commas inside numbers aren’t ambiguous
    decimal="."       # keep German-style decimals
)