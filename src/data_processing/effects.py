import pandas as pd
from src.data_access.local_reader import load_efficiency_rate

# Activity drivers = Mengeneffekt
# used to project consumption data and employees

# apply_efficiency_factor = Effizienzeffekt

def apply_efficiency_factor(consumption_data: pd.DataFrame, sector: str, energy_carrier: str, year: int) -> pd.DataFrame: #TODO: add petrol
    """
    Computes efficiency enhancement factors for a given sector, energy_carrier, and year.
    DISS 4.5.2 Modellierung des Effizienzeffekts

    Parameters:
        sector (str): 'cts' or 'industry'
        energy_carrier (str): 'power' or 'gas'
        year (int): Year from 2000 to 2050

    Returns:
        pd.DataFrame: Efficiency scaling factors for the given year
    """



    # validate inputs
    if year > 2050:
        raise ValueError("`year` must be lower than or equal to 2050.")
    if sector not in ["cts", "industry"]:
        raise ValueError("Sector must be either 'cts' or 'industry'")
    if energy_carrier not in ["power", "gas"]:
        raise ValueError("energy_carrier must be either 'power' or 'gas'")

    eff_rate = load_efficiency_rate(sector, energy_carrier)


    if year <= 2019:
        # our base year is 2018, below that we have no efficiency enhancements
        efficiency_factor = pd.DataFrame(1.0, index=eff_rate.columns, columns=eff_rate.index).transpose()
    else:
        # if year is in the future, function returns a df with calculated enhancement-levels based on year 2019
        years_phase1 = min(year - 2019, 2035 - 2019)
        years_phase2 = max(year - 2035, 0)

        efficiency_factor = (
            # pow(): pow(2, 3) = 2^3
            pow(1 - eff_rate.iloc[0], years_phase1) * pow(1 - eff_rate.iloc[1], years_phase2)
        )
    print(efficiency_factor)

    """
    gas industry:


    gas cts
    power industry:
    power cts
    """

    # apply the efficiency factor to the consumption data
    consumption_data = consumption_data.mul(efficiency_factor, axis=1)
    


    return efficiency_factor


