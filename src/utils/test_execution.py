import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from src.data_processing.employees import *
from src.data_processing.consumption import *
from src.data_access.api_reader import *
from src.pipeline.pipe_consumption import *
from src.data_access.local_reader import *
from src.data_processing.application import *


x = 16


if x == 1:

    df1 = load_decomposition_factors_power()
    print(df1)
    df2 = load_decomposition_factors_gas()
    print(df2)
    df3 = load_decomposition_factors_temperature_industry()
    print(df3)

elif x == 2:
    df = get_ugr_data_ranges(year=2021)

elif x == 3:
    df = get_manufacturing_energy_consumption(year=2002)

elif x == 4:
    df = get_historical_employees(year=2002)

elif x == 5:
    df = get_historical_employees_by_industry_sector_and_regional_id(year=2015)
    """ checked 
    get_historical_employees_by_industry_sector_and_regional_id(year=2015) returns the same as data.employees_per_branch(year=2015)
    -> 31326316.267379172
    """

elif x == 6:
    df = get_future_employees(year=2040)

elif x == 7:
    df = get_future_employees_by_industry_sector_and_regional_id(year=2033, force_preprocessing=True)
    """ checked
    get_future_employees_by_industry_sector_and_regional_id(year=2033, force_preprocessing=True)  -> 30544075.88298098
    data.employees_per_branch(year=2033) -> 30544075.88298098 
    """

elif x == 8:
    df = get_employees_per_industry_sector_groups_and_regional_ids(year=2033)

elif x == 9:
    df = get_employees_per_industry_sector_and_regional_ids(year=2015)

elif x == 10:
    df = get_consumption_data_historical(year=2015)

elif x == 11:
    df = project_consumption(year_dataset=2015, year_future=2033)

elif x == 12:
    df = get_consumption_data_future(year=2033)

elif x == 13:
    df = get_total_gas_industry_self_consuption(2015, force_preprocessing=True)

elif x == 14:
    df = get_regional_energy_consumption(year=2015)

elif x == 15:
    df = load_factor_gas_no_selfgen_cache(year=2015)

elif x == 16:
    sector = "cts"
    energy_carrier = "power"
    year = 2015

    
    consumption_data = get_employees_per_industry_sector_and_regional_ids(year=year)
    
    sectors_industry_sectors = dict_cts_or_industry_per_industry_sector()[sector]
    consumption_data = consumption_data.loc[consumption_data.index.intersection(sectors_industry_sectors)]

    df = dissaggregate_for_applications(consumption_data=consumption_data, year=year, sector=sector, energy_carrier=energy_carrier)

else:
    print("x is not 1")
    
print("python version: ", sys.version)
print("pandas version: ", pd.__version__)

    