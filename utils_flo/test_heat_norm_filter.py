import pandas as pd
import numpy as np

heat_norm = pd.DataFrame({
    'A': [1,   2,   13, 4],
    'B': [5,   3, 7,   8]
})

# temp_allo is the same shape
temp_allo = pd.DataFrame({
    'A': [10,  20,  14,   30],
    'B': [20,  25,  10,   5 ]
})



heat_norm = heat_norm[temp_allo[temp_allo > 15].isnull()].fillna(0)

print(heat_norm)




#Create the mask: True where temperature > 15
temp_above_15_mask = temp_allo > 15.0
# Keep only the values in heat_norm where temp > 15, else set to 0.0
heat_norm_masked = heat_norm.where(temp_above_15_mask).fillna(0)

print(heat_norm_masked)