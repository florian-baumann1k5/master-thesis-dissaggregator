import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.pipeline.pipe_heat import *



def main():
    """Function to run the fuel switch from petrol to power"""

    energy_carrier = "petrol"
    sector = "industry"
    switch_to = "power"


    for year in [2025, 2035, 2045]:
        for state in federal_state_dict().values():
            df = temporal_elec_load_from_fuel_switch(year=year, state=state, energy_carrier=energy_carrier, sector=sector, switch_to=switch_to, force_preprocessing=True)

            print(df)

if __name__ == "__main__":
    main()


