
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import geopandas as gpd
from matplotlib.patches import Rectangle
from mpl_toolkits.axes_grid1 import make_axes_locatable
import pandas as pd
import os



import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

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
from src.pipeline.pipe_ev_temporal import *

now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


##################### old temp code ######################################################

def old_whole_year_2018():
    file_path = 'src/utils/thesis_outputs/df_output_oldcode_cts_gas_2018_temp.csv'
    df = pd.read_csv(file_path, index_col=0, parse_dates=True)

    # Plot setup
    plt.figure(figsize=(15, 6))

    # Plot each meter's data
    for col in df.columns:
        plt.plot(df.index, df[col], label=f'Meter {col}', linewidth=0.8)

    # Labels and formatting
    plt.title('Gas Consumption Over Year 2018')
    plt.xlabel('Date')
    plt.ylabel('Consumption')
    plt.legend(loc='upper right', fontsize='small', ncol=2)
    plt.tight_layout()

    # Show plot
    plt.show()






##################### old temp code may only ######################################################

def old_may_2018():
    # Load data
    file_path = 'src/utils/thesis_outputs/df_output_oldcode_cts_gas_2018_temp.csv'
    df = pd.read_csv(file_path, index_col=0, parse_dates=True)

    df.index = pd.to_datetime(df.index)

    # Filter for May 2018
    df_may = df[(df.index >= '2018-05-01') & (df.index < '2018-06-01')]

    # Plot setup
    plt.figure(figsize=(15, 6))

    # Plot each meter's data
    for col in df_may.columns:
        plt.plot(df_may.index, df_may[col], label=f'Meter {col}', linewidth=0.8)

    # Labels and formatting
    plt.title('Gas Consumption in May 2018')
    plt.xlabel('Date')
    plt.ylabel('Consumption')
    plt.legend(loc='upper right', fontsize='small', ncol=2)
    plt.tight_layout()

    # Show plot
    plt.show()

old_may_2018()