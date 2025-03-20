import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))



from src.data_processing.consumption import generate_specific_consumption_per_branch


x = 1
if x == 1:
    generate_specific_consumption_per_branch()
else:
    print("x is not 1")
    
print(sys.version)
print(pd.__version__)

    