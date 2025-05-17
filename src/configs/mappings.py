def wz_dict():
    """
    Translate the openffe industry sector codes to real industry sector codes or groups.
    !!! Since switching from openffe UGR to GENISIS, 37 und 38-39 (opfenffe) will be 37-39 (genisis)

    keys     = openFFE lable
    values   = our internal label [48 unique industry sector code groups]
    """
    return {2: '1', 3: '2', 4: '3', 6: '5', 7: '6', 8: '7-9', 10: '10-12',
            11: '13-15', 12: '16', 13: '17', 14: '18', 15: '19', 18: '20',
            19: '21', 20: '22', 21: '23', 24: '24', 28: '25', 29: '26',
            30: '27', 31: '28', 32: '29', 33: '30', 34: '31-32', 35: '33',
            36: '35', 40: '36', 42: '37-39', 43: '37-39', 45: '41-42', 46: '43',
            48: '45', 49: '46', 50: '47', 52: '49', 53: '49', 54: '50',
            55: '51', 56: '52', 57: '53', 58: '55-56', 59: '58-63',
            60: '64-66', 61: '68', 62: '69-75', 63: '77-82', 64: '84',
            65: '85', 66: '86-88', 67: '90-99'}


def industry_sector_groups():
    """
    Returns a unique list of industry sector groups. (48 groups)
    """
    return list(set(wz_dict().values()))


def dict_cts_or_industry_per_industry_sector():
    """
    Defines which industry_sector is a CTS or industry sector
    Returns a dict with two keys: 'industry' and 'cts'. The values are lists of industry_sector codes.

    !!! Warning:
       - WZ 35 is missing
    """
    return {
        'industry': list(range(5, 34)), # = [5, 6, ..., 33] -> length 29
        'cts': [1, 2, 3, 36, 37, 38, 39, 41, 42, 43, 45, 46, 47, 49, 50, 51, 52,
              53, 55, 56, 58, 59, 60, 61, 62, 63, 64, 65, 66, 68, 69, 70, 71,
              72, 73, 74, 75, 77, 78, 79, 80, 81, 82, 84, 85, 86, 87, 88, 90,
              91, 92, 93, 94, 95, 96, 97, 98, 99] # -> length 58
    }


def federal_state_dict() -> dict:
    """
    Translate the federal state (Bundesland) number to its abbreviation.
    """
    return {1: 'SH', 2: 'HH', 3: 'NI', 4: 'HB', 5: 'NW', 6: 'HE',
            7: 'RP', 8: 'BW', 9: 'BY', 10: 'SL', 11: 'BE', 12: 'BB',
            13: 'MV', 14: 'SN', 15: 'ST', 16: 'TH'}


def shift_profile_industry():
    """
    Assign a shift profile to every industry_sector.
    """
    return {5: 'S3_WT_SA', 6: 'S3_WT_SA_SO', 7: 'S3_WT_SA', 8: 'S3_WT_SA',
            9: 'S3_WT_SA', 10: 'S2_WT', 11: 'S3_WT', 12: 'S3_WT_SA',
            13: 'S2_WT', 14: 'S2_WT', 15: 'S2_WT_SA', 16: 'S2_WT_SA',
            17: 'S3_WT_SA_SO', 18: 'S3_WT_SA_SO', 19: 'S3_WT_SA_SO',
            20: 'S3_WT_SA_SO', 21: 'S3_WT_SA_SO', 22: 'S2_WT_SA',
            23: 'S3_WT_SA_SO', 24: 'S3_WT_SA_SO', 25: 'S3_WT', 26: 'S2_WT',
            27: 'S2_WT_SA', 28: 'S2_WT', 29: 'S3_WT', 30: 'S3_WT_SA_SO',
            31: 'S1_WT_SA', 32: 'S3_WT_SA_SO', 33: 'S2_WT_SA'}

def hist_weather_year():
    """
    Assign temperature data of a historical year to a future year.
    """
    return {2000: 2008, 2001: 2009, 2002: 2010, 2003: 2011, 2004: 2012,
            2005: 2013, 2006: 2006, 2007: 2007, 2008: 2008, 2009: 2009,
            2010: 2010, 2011: 2011, 2012: 2012, 2013: 2013, 2014: 2014,
            2015: 2015, 2016: 2012, 2017: 2017, 2018: 2018, 2019: 2007,
            2020: 2008, 2021: 2009, 2022: 2010, 2023: 2011, 2024: 2012,
            2025: 2013, 2026: 2014, 2027: 2015, 2028: 2012, 2029: 2017,
            2030: 2018, 2031: 2006, 2032: 2008, 2033: 2009, 2034: 2010,
            2035: 2011, 2036: 2008, 2037: 2013, 2038: 2014, 2039: 2015,
            2040: 2012, 2041: 2017, 2042: 2018, 2043: 2007, 2044: 2008,
            2045: 2009, 2046: 2010, 2047: 2011, 2048: 2012, 2049: 2013,
            2050: 2014}




def gas_load_profile_parameters_dict():
    """
    Assign parameters to gas load profiles.
    """
    return {'A': {'BA': 0.277008711731108, 'BD': 1.4633681573375,
                  'BH': 0.987428301992787, 'GA': 1.15820816823062,
                  'GB': 1.82137779524266, 'HA': 1.97247753750471,
                  'KO': 1.35545152289308, 'MF': 1.23286546541232,
                  'MK': 1.42024191542431, 'PD': 1.71107392562331,
                  'WA': 0.333783832123808, 'SpaceHeating-MFH': 1.2328655,
                  'SpaceHeating-EFH': 1.6209544,
                  'Cooking_HotWater-HKO': 0.4040932},
            'B': {'BA': -33.0, 'BD': -36.17941165, 'BH': -35.25321235,
                  'GA': -36.28785839, 'GB': -37.5, 'HA': -36.96500652,
                  'KO': -35.14125631, 'MF': -34.72136051, 'MK': -34.88061302,
                  'PD': -35.8, 'WA': -36.02379115,
                  'SpaceHeating-MFH': -34.7213605,
                  'SpaceHeating-EFH': -37.1833141,
                  'Cooking_HotWater-HKO': -24.4392968},
            'C': {'BA': 5.72123025, 'BD': 5.926516165, 'BH': 6.154440641,
                  'GA': 6.588512639, 'GB': 6.346214795, 'HA': 7.225694671,
                  'KO': 7.130339509, 'MF': 5.816430402, 'MK': 6.595189922,
                  'PD': 8.4, 'WA': 4.866274683, 'SpaceHeating-MFH': 5.8164304,
                  'SpaceHeating-EFH': 5.6727847,
                  'Cooking_HotWater-HKO': 6.5718175},
            'D': {'BA': 0.4865118291885, 'BD': 0.0808834761578303,
                  'BH': 0.226571574644788, 'GA': 0.223568019279065,
                  'GB': 0.0678117914984112, 'HA': 0.0345781570412447,
                  'KO': 0.0990618615825365, 'MF': 0.0873351930206002,
                  'MK': 0.038531702714089, 'PD': 0.0702545839208687,
                  'WA': 0.491227957971774, 'SpaceHeating-MFH': 0.0873352,
                  'SpaceHeating-EFH': 0.0716431,
                  'Cooking_HotWater-HKO': 0.7107710},
            'mH': {'BA': -0.00948491309440127, 'BD': -0.047579990370696,
                   'BH': -0.0339019728779373, 'GA': -0.0410334784248699,
                   'GB': -0.0607665689685263, 'HA': -0.0742174022298938,
                   'KO': -0.0526486914295292, 'MF': -0.0409283994003907,
                   'MK': -0.0521084240793636, 'PD': -0.0745381134111297,
                   'WA': -0.0092263492839078, 'SpaceHeating-MFH': -0.0409284,
                   'SpaceHeating-EFH': -0.0495700,
                   'Cooking_HotWater-HKO': 0},
            'bH': {'BA': 0.463023693687715, 'BD': 0.82307541850402,
                   'BH': 0.693823369584483, 'GA': 0.752645138542657,
                   'GB': 0.930815856582958, 'HA': 1.04488686764057,
                   'KO': 0.862608575142234, 'MF': 0.767292039450741,
                   'MK': 0.864791873696473, 'PD': 1.04630053886108,
                   'WA': 0.45957571089625, 'SpaceHeating-MFH': 0.7672920,
                   'SpaceHeating-EFH': 0.8401015,
                   'Cooking_HotWater-HKO': 0},
            'mW': {'BA': -0.000713418600565782, 'BD': -0.00192725690584626,
                   'BH': -0.00128490078017325, 'GA': -0.000908768552979623,
                   'GB': -0.00139668882761774, 'HA': -0.000829544720239446,
                   'KO': -0.000880838956026602, 'MF': -0.00223202741619469,
                   'MK': -0.00143692105046127, 'PD': -0.000367207932817838,
                   'WA': -0.000967642449895133, 'SpaceHeating-MFH': -0.0022320,
                   'SpaceHeating-EFH': -0.0022090,
                   'Cooking_HotWater-HKO': 0},
            'bW': {'BA': 0.386744669887959, 'BD': 0.107704598925155,
                   'BH': 0.202973165694549, 'GA': 0.191664070308203,
                   'GB': 0.0850398799492811, 'HA': 0.0461794912976014,
                   'KO': 0.0964014193937084, 'MF': 0.119920720218609,
                   'MK': 0.0637601910393071, 'PD': 0.0621882262236128,
                   'WA': 0.396429075178636, 'SpaceHeating-MFH': 0.1199207,
                   'SpaceHeating-EFH': 0.1074468,
                   'Cooking_HotWater-HKO': 0},
            'MO': {'BA': 1.0848, 'BD': 1.1052, 'BH': 0.9767, 'GA': 0.9322,
                   'GB': 0.9897, 'HA': 1.0358, 'KO': 1.0354, 'MF': 1.0354,
                   'MK': 1.0699, 'PD': 1.0214, 'WA': 1.2457,
                   'SpaceHeating-MFH': 1.0,
                   'SpaceHeating-EFH': 1.0,
                   'Cooking_HotWater-HKO': 1.0},
            'DI': {'BA': 1.1211, 'BD': 1.0857, 'BH': 1.0389, 'GA': 0.9894,
                   'GB': 0.9627, 'HA': 1.0232, 'KO': 1.0523, 'MF': 1.0523,
                   'MK': 1.0365, 'PD': 1.0866, 'WA': 1.2615,
                   'SpaceHeating-MFH': 1.0,
                   'SpaceHeating-EFH': 1.0,
                   'Cooking_HotWater-HKO': 1.0},
            'MI': {'BA': 1.0769, 'BD': 1.0378, 'BH': 1.0028, 'GA': 1.0033,
                   'GB': 1.0507, 'HA': 1.0252, 'KO': 1.0449, 'MF': 1.0449,
                   'MK': 0.9933, 'PD': 1.072, 'WA': 1.2707,
                   'SpaceHeating-MFH': 1.0,
                   'SpaceHeating-EFH': 1.0,
                   'Cooking_HotWater-HKO': 1.0},
            'DO': {'BA': 1.1353, 'BD': 1.0622, 'BH': 1.0162, 'GA': 1.0109,
                   'GB': 1.0552, 'HA': 1.0295, 'KO': 1.0494, 'MF': 1.0494,
                   'MK': 0.9948, 'PD': 1.0557, 'WA': 1.243,
                   'SpaceHeating-MFH': 1.0,
                   'SpaceHeating-EFH': 1.0,
                   'Cooking_HotWater-HKO': 1.0},
            'FR': {'BA': 1.1402, 'BD': 1.0266, 'BH': 1.0024, 'GA': 1.018,
                   'GB': 1.0297, 'HA': 1.0253, 'KO': 0.9885, 'MF': 0.9885,
                   'MK': 1.0659, 'PD': 1.0117, 'WA': 1.1276,
                   'SpaceHeating-MFH': 1.0,
                   'SpaceHeating-EFH': 1.0,
                   'Cooking_HotWater-HKO': 1.0},
            'SA': {'BA': 0.4852, 'BD': 0.7629, 'BH': 1.0043, 'GA': 1.0356,
                   'GB': 0.9767, 'HA': 0.9675, 'KO': 0.886, 'MF': 0.886,
                   'MK': 0.9362, 'PD': 0.9001, 'WA': 0.3877,
                   'SpaceHeating-MFH': 1.0,
                   'SpaceHeating-EFH': 1.0,
                   'Cooking_HotWater-HKO': 1.0},
            'SO': {'BA': 0.9565000000000001, 'BD': 0.9196,
                   'BH': 0.9587000000000012, 'GA': 1.0106000000000002,
                   'GB': 0.9352999999999998, 'HA': 0.8935000000000004,
                   'KO': 0.9434999999999993, 'MF': 0.9434999999999993,
                   'MK': 0.9033999999999995, 'PD': 0.8524999999999991,
                   'WA': 0.4638,
                   'SpaceHeating-MFH': 1.0,
                   'SpaceHeating-EFH': 1.0,
                   'Cooking_HotWater-HKO': 1.0}}


def load_profiles_cts_gas():
    """
    Assign a gas load profile (SLP) to each CTS branch by WZ number.
    """
    return {1: 'GB', 2: 'GB', 3: 'GB', 36: 'MF', 37: 'MF', 38: 'BD', 39: 'BD',
            41: 'MK', 42: 'MK', 43: 'MK', 45: 'MK', 46: 'HA', 47: 'HA',
            49: 'BD', 50: 'GA', 51: 'GA', 52: 'BD', 53: 'KO', 55: 'BH',
            56: 'GA', 58: 'PD', 59: 'BD', 60: 'KO', 61: 'BD', 62: 'BD',
            63: 'BD', 64: 'KO', 65: 'KO', 66: 'KO', 68: 'BD', 69: 'BD',
            70: 'BD', 71: 'BD', 72: 'KO', 73: 'KO', 74: 'BD', 75: 'BD',
            77: 'BD', 78: 'KO', 79: 'BD', 80: 'BD', 81: 'BD', 82: 'BD',
            84: 'KO', 85: 'KO', 86: 'BH', 87: 'KO', 88: 'MF', 90: 'BD',
            91: 'KO', 92: 'BD', 93: 'KO', 94: 'KO', 95: 'MK', 96: 'BD',
            97: 'MF', 98: 'MF', 99: 'KO'}

def load_profiles_cts_power():
    """
    Assign a power load profile (SLP) to every CTS branch by WZ number.
    """
    return {1: 'L0', 2: 'L0', 3: 'G3', 35: 'G3', 36: 'G3', 37: 'G3',
            38: 'G3', 39: 'G3', 41: 'G1', 42: 'G1', 43: 'G1', 45: 'G4',
            46: 'G0', 47: 'G0', 49: 'G3', 50: 'G3', 51: 'G3', 52: 'G3',
            53: 'G4', 55: 'G2', 56: 'G2', 58: 'G1', 59: 'G0', 60: 'G3',
            61: 'G3', 62: 'G3', 63: 'G3', 64: 'G1', 65: 'G1', 66: 'G1',
            68: 'G1', 69: 'G1', 70: 'G1', 71: 'G1', 72: 'G1', 73: 'G1',
            74: 'G1', 75: 'G1', 77: 'G4', 78: 'G1', 79: 'G4', 80: 'G3',
            81: 'L0', 82: 'G0', 84: 'G1', 85: 'G1', 86: 'G3', 87: 'G2',
            88: 'H0', 90: 'G0', 91: 'G0', 92: 'G2', 93: 'G2', 94: 'G6',
            95: 'G4', 96: 'G1', 97: 'H0', 98: 'H0', 99: 'G1'}

# Translation
def translate_application_columns_mapping() -> list:
    return {
       # power decomposition
       'Beleuchtung':                           'lighting',
       'IKT':                                   'information_communication_technology',
       'Klimakälte':                            'space_cooling',
       'Prozesskälte':                          'process_cooling',
       'Mechanische Energie':                   'mechanical_energy',
       'Prozesswärme':                          'process_heat',
       'Raumwärme':                             'space_heating',
       'Warmwasser':                            'hot_water',
       'Strom Netzbezug':                       'electricity_grid',
       'Strom Eigenerzeugung':                  'electricity_self_generation',

       # temperature‑industry decomposition
       'Prozesswärme <100°C':                   'process_heat_below_100C',
       'Prozesswärme 100°C-200°C':              'process_heat_100_to_200C',
       'Prozesswärme 200°C-500°C':              'process_heat_200_to_500C',
       'Prozesswärme >500°C':                   'process_heat_above_500C',

       # gas decomposition
       'Anteil Erdgas am Verbrauch aller Gase': 'share_natural_gas_total_gas',
       'Energetischer Erdgasverbrauch':         'natural_gas_consumption_energetic',
       'Nichtenergetische Nutzung':             'non_energetic_use',

       'Industriekraftwerke':                  'industry_power_plants',
       'WZ':                                    'industry_sector'
    }

def get_efficiency_level_by_application_gas(application):
    """
    old fct: get_efficiency_level()
    
    Returns value for given key from dictionary with efficiencies of
    gas applications.

    Args:
        application : str
    
    Returns:
        float : efficiency level for given application

    """
    eff_gas_dict = {
                    'mechanical_energy': 0.4,
                    'non_energetic_use': 0.7,
                    'process_heat': 0.96,
                    'process_heat_100_to_200C': 0.9,
                    'process_heat_200_to_500C': 0.9,
                    'process_heat_below_100C': 0.96,
                    'process_heat_above_500C': 0.8,
                    'space_heating': 0.96,
                    'hot_water': 0.96}
    try:
        return eff_gas_dict[application]
    except KeyError:
        raise ValueError(f"No gasefficiency level found for application: {application}")


def get_efficiency_level_by_application_petrol(application):
    """    
    Returns value for given key from dictionary with efficiencies of
    petrol applications.

    Args:
        application : str
    
    Returns:
        float : efficiency level for given application

    """
    eff_petrol_dict = {
                    'mechanical_energy': 0.4,
                    'process_heat': 0.96,
                    'process_heat_100_to_200C': 0.9,
                    'process_heat_200_to_500C': 0.9,
                    'process_heat_below_100C': 0.96,
                    'process_heat_above_500C': 0.8,
                    'space_heating': 0.96,
                    'hot_water': 0.96}
    try:
        return eff_petrol_dict[application]
    except KeyError:
        raise ValueError(f"No petrol efficiency level found for application: {application}")
    
def get_heatpump_distribution() -> dict:
    """
    Get the distribution of heat pumps (ground, air, water) by energy carrier.
    """
    return {
        "p_ground": 0.36,
        "p_air": 0.58,
        "p_water": 0.06
    }

# EVS
def get_total_car_stock() -> int:
    """
    Get the total car stock for the given year.
    Data coming from the 2024 Kraftfahrt-Bundesamt registration statistics (FZ 1.2).
    """
    total_car_stock_germany_2024 = 49339166
    return total_car_stock_germany_2024

