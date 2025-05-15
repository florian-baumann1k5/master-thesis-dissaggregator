import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from src.data_access.local_reader import *
from src.pipeline.pipe_ev_regional_consumption import *
from src.data_processing.electric_vehicles import *
from src.utils.utils import *
from src.pipeline.pipe_ev_temporal import *



x = 9


if x == 1:
    df = historical_electric_vehicle_consumption(year=2024)
    print(df)

if x == 2:
    df = calculate_existing_ev_stock(year=2024)
    print(df)
if x == 3:
    evs = s1_future_ev_stock_15mio_by_2030(2036)
    print(evs)
if x == 4:
    df = historical_electric_vehicle_consumption(year=2024)
    df1 = future_1_electric_vehicle_consumption(year=2025)
    print(df)
if x == 5:
    df = future_2_electric_vehicle_consumption(year=2028, szenario="ambit")
    print(df)
if x == 6:   # check total ev consumption by regional id
    df = electric_vehicle_consumption_by_regional_id(year=2036, szenario="UGR")
    df = electric_vehicle_consumption_by_regional_id(year=2016, szenario="UGR")

    df = electric_vehicle_consumption_by_regional_id(year=2013, szenario="KBA_1")
    df = electric_vehicle_consumption_by_regional_id(year=2036, szenario="KBA_1")

    df = electric_vehicle_consumption_by_regional_id(year=2016, szenario="KBA_2")
    df = electric_vehicle_consumption_by_regional_id(year=2036, szenario="KBA_2", s2_szenario="ambit")
    print(df)

elif x == 7:
    df = create_weekday_workday_holiday_mask(state="TH", year=2024)
    print(df)

elif x == 8:
    df = get_future_vehicle_consumption_ugr_by_energy_carrier(year=2036)
    print(df)

elif x == 9:
    year = 2045
    szenario = "UGR"
    s2_szenario = None
    df = electric_vehicle_consumption_by_region_id_and_temporal_resolution(year=year, szenario=szenario, s2_szenario=s2_szenario)
    print(df)

else:
    None



