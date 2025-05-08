import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from src.data_access.local_reader import *
from src.pipeline.pipe_electric_vehicles import *
from src.data_processing.electric_vehicles import *

x = 4


if x == 1:
    df = historical_electric_vehicle_consumption(year=2024)
    print(df)

if x == 2:
    df = calculate_existing_ev_stock(year=2024)
    print(df)
if x == 3:
    evs = ev_stock_15mio_by_2030(2036)
    print(evs)
if x == 4:
    df = historical_electric_vehicle_consumption(year=2024)
    df1 = future_1_electric_vehicle_consumption(year=2025)
    print(df)

