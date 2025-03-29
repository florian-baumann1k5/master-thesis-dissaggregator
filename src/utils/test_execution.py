import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from src.data_processing.employees import (get_historical_employees_by_industry_sector_and_regional_id, 
                                            get_future_employees_by_industry_sector_and_regional_id, 
                                            get_employees_per_industry_sector_groups_and_regional_ids, 
                                            get_employees_per_industry_sector_and_regional_ids)
from src.data_processing.consumption import (get_ugr_data_ranges,
                                             project_consumption,
                                             get_total_gas_industry_self_consuption,
                                             get_regional_energy_consumption)
from src.data_access.api_reader import get_historical_employees, get_future_employees, get_manufacturing_energy_consumption
from src.pipeline.consumption import get_historical_consumption, get_future_consumption

x = 14
if x == 1:
    # local_reader.py /api_reader.py
    df = get_manufacturing_energy_consumption(year=2015)

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
    df = get_employees_per_industry_sector_and_regional_ids(year=2033)

elif x == 10:
    df = get_historical_consumption(year=2010)

elif x == 11:
    df = project_consumption(year_dataset=2015, year_future=2033)

elif x == 12:
    df = get_future_consumption(year=2033)

elif x == 13:
    df = get_total_gas_industry_self_consuption(2015, force_preprocessing=True)

elif x == 14:
    df = get_regional_energy_consumption(year=2015)

else:
    print("x is not 1")
    
print("python version: ", sys.version)
print("pandas version: ", pd.__version__)

    