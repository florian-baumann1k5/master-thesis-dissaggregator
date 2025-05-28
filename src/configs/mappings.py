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






    






