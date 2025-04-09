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
    """
    return {
        'industry': list(range(5, 34)), # = [5, 6, ..., 33] 
        'cts': [1, 2, 3, 36, 37, 38, 39, 41, 42, 43, 45, 46, 47, 49, 50, 51, 52,
              53, 55, 56, 58, 59, 60, 61, 62, 63, 64, 65, 66, 68, 69, 70, 71,
              72, 73, 74, 75, 77, 78, 79, 80, 81, 82, 84, 85, 86, 87, 88, 90,
              91, 92, 93, 94, 95, 96, 97, 98, 99]
    }


def federal_state_dict():
    """
    Translate the federal state (Bundesland) number to its abbreviation.
    """
    return {01: 'SH', 02: 'HH', 03: 'NI', 04: 'HB', 05: 'NW', 06: 'HE',
            07: 'RP', 08: 'BW', 09: 'BY', 10: 'SL', 11: 'BE', 12: 'BB',
            13: 'MV', 14: 'SN', 15: 'ST', 16: 'TH'}