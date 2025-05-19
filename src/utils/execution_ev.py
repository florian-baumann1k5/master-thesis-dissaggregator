import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.data_processing.employees import *
from src.data_processing.consumption import *
from src.data_access.api_reader import *
from src.pipeline.pipe_consumption import *
from src.pipeline.pipe_applications import *
from src.data_access.local_reader import *
from src.data_processing.application import *
from src.data_processing.temporal import *
from src.data_processing.temperature import *
from src.pipeline.pipe_temporal import *
from src.pipeline.pipe_heat import *
from src.data_processing.heat import *
from src.data_processing.cop import *
from src.pipeline.pipe_ev_regional_consumption import *


path = "src/utils/thesis_outputs"

def test():
    print("Done")

    df_total = pd.DataFrame()

    for year in range(2017, 2046):
        df = electric_vehicle_consumption_by_regional_id(year=year, szenario="KBA_2", s2_szenario="trend")
        total_consumption = df.sum().sum()
        df_total = pd.concat([df_total, pd.DataFrame({"year": [year], "total_consumption": [total_consumption]})])

    df_total.to_csv(os.path.join(path, "ev_consumption_by_regional_id_kba_2_trend.csv"), index=False)

test()