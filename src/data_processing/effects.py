import pandas as pd
from src.data_access.local_reader import *
from src.utils.utils import *
from src import logger

# Activity drivers = Mengeneffekt
# used to project consumption data and employees

# apply_efficiency_factor = Effizienzeffekt
# apply_activity_driver = Mengeneffekt (= project consumption data into the future)

def apply_efficiency_factor(consumption_data: pd.DataFrame, sector: str, energy_carrier: str, year: int) -> pd.DataFrame: #TODO: add petrol
    """
    Computes efficiency enhancement factors for a given sector, energy_carrier, and year.
    DISS 4.5.2 Modellierung des Effizienzeffekts

    Watch out:
    For industry we have the efficiency enhancement factors for each industry_sector.
    For cts sectors we have the efficiency enhancement factors for the applications.

    Parameters:
        sector (str): 'cts' or 'industry'
        energy_carrier (str): 'power' or 'gas'
        year (int): Year from 2000 to 2050

    Returns:
        pd.DataFrame: consumption data with efficiency enhancement factors applied
            Index: regional_id
            MultiIndex columns: 
                level=0: industry_sector
                level=1: application
    """

    # validate inputs
    if year > 2050:
        raise ValueError("`year` must be lower than or equal to 2050.")
    if sector not in ["cts", "industry"]:
        raise ValueError("Sector must be either 'cts' or 'industry'")
    if energy_carrier not in ["power", "gas"]:
        raise ValueError("energy_carrier must be either 'power' or 'gas'")


    # load the efficiency rate
    eff_rate = load_efficiency_rate(sector, energy_carrier)


    # calculate the efficiency factor
    if year <= 2019:
        # our base year is 2018, below that we have no efficiency enhancements
        #efficiency_factor = pd.DataFrame(1.0, index=eff_rate.columns, columns=eff_rate.index).transpose()
        efficiency_factor = pd.Series(1.0, index=eff_rate.columns)
    else:
        # if year is in the future, function returns a df with calculated enhancement-levels based on year 2019
        years_phase1 = min(year - 2019, 2035 - 2019)
        years_phase2 = max(year - 2035, 0)

        efficiency_factor = (
            # pow(): pow(2, 3) = 2^3
            pow(1 - eff_rate.iloc[0], years_phase1) * pow(1 - eff_rate.iloc[1], years_phase2)
        )

    if not isinstance(efficiency_factor, pd.Series):
        raise ValueError("efficiency_factor is not a Series! Apply efficiency factor will fail.")

    """ efficiency_factor output examples:
    -> gas industry:
    industry_sector
    5     0.9810
    6     0.9810
    7     0.9810
    8     0.9810
    9     0.9810
    10    0.9810
    11    0.9810
    12    0.9810
    13    0.9810
    14    0.9810
    15    0.9810
    16    0.9810
    17    0.9970
    18    0.9810
    19    0.9980
    20    0.9980
    21    0.9810
    22    0.9810
    23    0.9950
    24    0.9975
    25    0.9810
    26    0.9810
    27    0.9810
    28    0.9810
    29    0.9810
    30    0.9810
    31    0.9810
    32    0.9810
    33    0.9810

    -> gas cts:
    mechanical_energy    0.985
    process_heat         0.987
    space_heating        0.975
    hot_water            0.975
    non_energetic_use    1.000

    -> power industry:
    industry_sector
    5     0.9810
    6     0.9810
    7     0.9810
    8     0.9810
    9     0.9810
    10    0.9810
    11    0.9810
    12    0.9810
    13    0.9810
    14    0.9810
    15    0.9810
    16    0.9810
    17    0.9970
    18    0.9810
    19    0.9980
    20    0.9980
    21    0.9810
    22    0.9810
    23    0.9950
    24    0.9975
    25    0.9810
    26    0.9810
    27    0.9810
    28    0.9810
    29    0.9810
    30    0.9810
    31    0.9810
    32    0.9810
    33    0.9810


    -> power cts:
    lighting                                0.979
    information_communication_technology    0.993
    space_cooling                           1.005
    process_cooling                         0.967
    mechanical_energy                       0.985
    process_heat                            0.987
    space_heating                           0.991
    hot_water                               0.991
    non_energetic_use                       1.000
    """

    # apply the efficiency factor to the consumption data
    # for industry per industry_sectors and for cts per applications
    if sector == "industry":
        consumption_data_efficiency_factor = consumption_data.mul(efficiency_factor, level=0, axis=1)

        sectors_in_data = consumption_data.columns.get_level_values(0).unique()
        sectors_in_factor = efficiency_factor.index.unique()
        missing_sectors = sectors_in_data.difference(sectors_in_factor)
        if not missing_sectors.empty:
            raise ValueError("Check the industry sectors names! Missing industry efficiency factors for:", missing_sectors.tolist())


    elif sector == "cts":
        consumption_data_efficiency_factor = consumption_data.mul(efficiency_factor, level=1, axis=1)

        # check for missing application efficiency factors
        applications_in_data = consumption_data.columns.get_level_values(1).unique()
        applications_in_factor = efficiency_factor.index.unique()
        missing_apps = applications_in_data.difference(applications_in_factor)
        if not missing_apps.empty:
            raise ValueError("Check the application names! Missing application efficiency factors for:", missing_apps.tolist())


    return consumption_data_efficiency_factor


def apply_activity_driver(consumption: pd.DataFrame, year_dataset: int, year_future: int):
    """
    DISS 4.5
    Project energy demand per wz to given year using activity drivers from
    input files. For industry gross value added (gva) per branch is used, for
    CTS energy reference area per branch is used, which is derived from
    projected number of employees

    Projects consumption data from publications to future year using different
    demand driver per sector. for industry use projected gross value added per
    branch and for CTS use projected consumption area per branch. drivers are
    imported from data_in folder.

    Args:
        consumption (pd.DataFrame): consumption data
        year_dataset (int): year of the latest UGR data
        year_future (int): year in the future to project the consumption to

    Returns:
        pd.DataFrame: consumption data with activity driver applied
            Index: industry_sector
            Columns: ['power[MWh]', 'gas[MWh]', 'petrol[MWh]']
    """

    # Validate Inputs: activity drivers are only available for 2015-2050
    if year_dataset not in range(2015, 2051) or year_future not in range(2015, 2051):
        raise ValueError("year_dataset must be between 2015 and 2050. Use the historical consumption!")
    
    # 2. Get Activity Drivers = Mengeneffekt
    activity_drivers = load_activity_driver_consumption()

    # 3. group industry sectors
    df_driver_total = group_activity_drivers(df_driver_total= activity_drivers, columns=consumption.index)
    #df_driver_total = activity_drivers

    # 4. normalize activity drivers year_dataset
    df_driver_norm = df_driver_total.apply(lambda x: x/x.loc[year_dataset])

    # 5. get the wanted year
    df_driver_norm_year = df_driver_norm.loc[year_future]

    # 6. multiply with consumption
    consumption_projected = consumption.mul(df_driver_norm_year, axis=0)

    # 7. set empty values to 0
    #consumption_projected = consumption_projected.fillna(0)

    return consumption_projected