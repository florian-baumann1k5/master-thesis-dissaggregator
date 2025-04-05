import pandas as pd
from src.data_access.local_reader import load_efficiency_rate

# Activity drivers = Mengeneffekt
# used to project consumption data and employees

def load_efficiency_factor(sector: str, source: str, year: int) -> pd.DataFrame:
    """
    Computes efficiency enhancement factors for a given sector, source, and year.

    Parameters:
        sector (str): 'CTS' or 'industry'
        source (str): 'power' or 'gas'
        year (int): Year from 2000 to 2050

    Returns:
        pd.DataFrame: Efficiency scaling factors for the given year
    """

    # validate inputs
    if year > 2050:
        raise ValueError("`year` must be lower than or equal to 2050.")

    eff_rate = load_efficiency_rate(sector, source)

    if year <= 2019:
        return pd.DataFrame(1.0, index=eff_rate.columns, columns=eff_rate.index).transpose()

    years_phase1 = min(year - 2019, 2035 - 2019)
    years_phase2 = max(year - 2035, 0)

    factor = (
        # pow(): pow(2, 3) = 2^3
        pow(1 - eff_rate.iloc[0], years_phase1) * pow(1 - eff_rate.iloc[1], years_phase2)
    )

    return factor


