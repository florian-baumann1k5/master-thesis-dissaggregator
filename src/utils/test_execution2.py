import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from src.data_access.local_reader import *
from src.pipeline.pipe_electric_vehicles import *

x = 2


if x == 1:
    df = load_registered_electric_vehicles_by_regional_id(year=2020)
    print(df)

if x == 2:
    df = historical_electric_vehicle_consumption(year=2000)
    print(df)




