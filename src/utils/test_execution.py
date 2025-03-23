import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))



from src.data_processing.consumption import generate_specific_consumption_per_branch
from src.data_processing.consumption import get_ugr_data_ranges
from src.data_access.api_reader import get_manufacturing_energy_consumption
from src.data_access.api_reader import get_historical_employees
from src.data_processing.employees import get_employees_by_industry_code_and_regional_code

x = 5
if x == 1:
    df = generate_specific_consumption_per_branch()

elif x == 2:
    df = get_ugr_data_ranges(year=2002)

elif x == 3:
    df = get_manufacturing_energy_consumption(year=2002)

elif x == 4:
    df = get_historical_employees(year=2002)

elif x == 5:
    df = get_employees_by_industry_code_and_regional_code(year=2002)

else:
    print("x is not 1")
    
print("python version: ", sys.version)
print("pandas version: ", pd.__version__)

    