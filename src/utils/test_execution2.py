import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from src.data_access.local_reader import *
from src.pipeline.pipe_electric_vehicles import *
from src.data_processing.electric_vehicles import *

x = 5


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

else:
    None



import pandas as pd

# anchor points only:
df = pd.DataFrame({
    'year': [2025, 2030, 2045],
    'value': [1.6, 15.0, 49.0]
}).set_index('year')

# reindex to fill every year
idx = range(2025, 2046)
df2 = df.reindex(idx)

# linear vs index
lin  = df2.interpolate(method='linear')
ind  = df2.interpolate(method='index')

# they match here because idx is uniform
print((lin - ind).dropna())