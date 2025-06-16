import pandas as pd

scenario_row= 2    # 0‑based row numbers in the worksheet
location_row = 3
first_data_row = 5

src_excel = "utils_flo/ev_ugr_to_csv/Standardlastprofile_Elektrofahrzeuge_Anhang_E.xlsx"



#for i in range(1, 7):

#sheet = f"HT{i}_Wochenende"
sheet = "Alle_Wochenende"
#sheet = "Alle_Werktag"
#x = sheet.split("_")[0].lower()
#day = "weekend"
#day = "workday"

x = "total"
day = "weekend"


raw = pd.read_excel(src_excel, sheet_name=sheet, header=None)


# delete the columns
raw = raw.drop(columns=[4,5,6,7,8,9])


# delete the first 5 rows
raw = raw.iloc[5:]

# rename the columns
raw.columns = ["time", "home_charging[kw/car]", "work_charging[kw/car]", "public_charging[kw/car]"]

# make the column "time" the index
raw = raw.set_index("time")



# make the index a time index
raw.index = raw.index.time

# name the index "time"
raw.index.name = "time"




dst_csv = f"utils_flo/ev_ugr_to_csv/ev_charging_profile_home_{x}_{day}.csv"


raw.to_csv(dst_csv, sep=";", decimal=".")
