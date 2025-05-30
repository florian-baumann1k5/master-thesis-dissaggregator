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


def decomp_factors_applications():
    """Get application disaggregation factors for industry petrol."""
    return get_application_dissaggregation_factors(sector="industry", energy_carrier="petrol")


def main():
    """Main execution function."""

    year = 2018
    energy_carrier = "petrol"
    force_preprocessing = True
    sector = "industry" 

    df = disagg_applications_efficiency_factor(
        sector=sector, 
        year=year, 
        energy_carrier=energy_carrier, 
        force_preprocessing=force_preprocessing
    )

    print(df)


if __name__ == "__main__":
    main()


