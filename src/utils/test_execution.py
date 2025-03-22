import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))



from src.data_processing.consumption import generate_specific_consumption_per_branch
from src.data_access.local_reader import get_ugr_data


x = 2
if x == 1:
    df = generate_specific_consumption_per_branch()
elif x == 2:
    df = get_ugr_data(year=2000)
else:
    print("x is not 1")
    
print("python version: ", sys.version)
print("pandas version: ", pd.__version__)

    