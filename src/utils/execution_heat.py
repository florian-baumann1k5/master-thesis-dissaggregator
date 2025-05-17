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



def switch():
    sector = "cts"
    #switch_to = "power"
    switch_to = "power"
    year = 2045

    df_gas_switch = sector_fuel_switch_fom_gas_petrol(sector=sector, switch_to=switch_to, year=year) # only returns apk "process_heat_above_500C" for every wz
    """
    idnex: regional_ids
    columns [0]: wz
    columns [1]: application (all 0 except for "process_heat_above_500C")
    """

    state = "TH"
    #df =create_heat_norm_cts(state=state, year=year)
    """
    index: timestamp
    columns [0]: state
    """

    print("Done")

    return None

switch()
