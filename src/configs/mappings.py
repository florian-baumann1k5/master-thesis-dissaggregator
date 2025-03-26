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