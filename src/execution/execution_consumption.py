import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from src.pipeline.pipe_consumption import *


def main():
    """Disaggregate the consumption to a level of industry sectors and regional_ids"""

    year = 2020
    energy_carrier = "petrol"
    force_preprocessing = True

    df = get_consumption_data(year=year, energy_carrier=energy_carrier, force_preprocessing=force_preprocessing)

    print(df)


if __name__ == "__main__":
    main()


