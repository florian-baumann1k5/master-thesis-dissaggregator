
# Temporal
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


# Heat
def get_efficiency_level_by_application_gas():
    """
    old fct: get_efficiency_level()
    
    Returns dictionary with efficiencies of gas applications.
    
    Returns:
        dict : efficiency level for each application

    """
    return {
        'mechanical_energy': 0.4,
        'non_energetic_use': 0.7,
        'process_heat': 0.96,
        'process_heat_100_to_200C': 0.9,
        'process_heat_200_to_500C': 0.9,
        'process_heat_below_100C': 0.96,
        'process_heat_above_500C': 0.8,
        'space_heating': 0.96,
        'hot_water': 0.96}

def get_efficiency_level_by_application_petrol():
    """    
    Returns dictionary with efficiencies of petrol applications.

    
    Returns:
        dict : efficiency level for given application

    """
    return {
        'mechanical_energy': 0.4,
        'non_energetic_use': 0.7,
        'process_heat': 0.96,
        'process_heat_100_to_200C': 0.9,
        'process_heat_200_to_500C': 0.9,
        'process_heat_below_100C': 0.96,
        'process_heat_above_500C': 0.8,
        'space_heating': 0.96,
        'hot_water': 0.96}

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

def get_efficiency_factor_by_fuel_type_compared_to_ev():
    """
    Returns dictionary with efficiencies of fuel types compared to BEVs.
    """
    return {
        'diesel[mwh]': 0.233,
        'biodiesel[mwh]': 0.233,
        'liquefied_petroleum_gas[mwh]': 0.273,
        'natural_gas[mwh]': 0.273,
        'petrol[mwh]': 0.313,
        'bioethanol[mwh]': 0.273,
        'biogas[mwh]': 0.273,
    }