import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from src.pipeline.pipe_applications import *


def main():
    """Disaggregate for applications"""

    year = 2018
    energy_carrier = "petrol"
    sector = "industry" 
    force_preprocessing = True

    df = disagg_applications_efficiency_factor(
        sector=sector, 
        year=year, 
        energy_carrier=energy_carrier, 
        force_preprocessing=force_preprocessing
    )

    print(df)


if __name__ == "__main__":
    main()


