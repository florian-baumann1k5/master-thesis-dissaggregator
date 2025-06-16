# DemandRegio Disaggregator

The DamendRegio project aims to disaggregate the final energy consumption of the sectors:
- Industry
- Commercial, Trade and Services (CTS)
- Private Household
into a high spacial and temporal resolution.

This project includes:
- The energy carriers: power, gas, petrol products for the industry and CTS sector
- Electricity consumption of the private households for electric vehicles


## Structure of the Disaggregator
### General
The Disaggregator is structured in the following way:
- src/pipeline/*: Contains the main functions of the Disaggregator. Pipeline functions combining the data_processing functions to generate wanted the data
- src/data_processing/*: Contains the data manipulation functions
- src/configs/*: Contains the configuration files and mappings
- data/raw/*: Contains the raw input data
- src/data_access/*: Contains the data access functions (API client and local file reader)
- src/utils/*: Contains the utility and execution functions
- data/processed/*: Contains cached data to avoid recomputing the data
- data/output/*: Contains the output data


### Approach
The Disaggregator for the industry and CTS sector follows a top down bottom up approach.
1. Consumption data is loaded from the raw data folder
2. In the consumption pipeline the consumption is disaggregated on a level of industry sectors and regional_ids
3. The disaggregated consumption is then used to disaggregate it on a level of applications
4. The yearly cosnumption by economic sector, applciation and regional id is then used to disaggregate it on a level of temporal resolution
5. To predict future consumption the effects in the src/data_processing/effects.py are used.
6. To model the fuel switch the src/data_pipelines/pipe_heat.py is used.

The Disaggregator for the EV consumption of private households follows two approaches. The KBA approach uses data of registered EVs and the UGR approach uses data of electricity usage in private households for transportation. To map the future the KVB approach uses either the political goals (15mio EVs by 2030 and only EVs by 2045) or projected number of EVs from literature. For the UGR approach we assume that the consumption of fossil energy carriers is replaced by electricity.
1. the regional disaggregated consumption (historical and future) is calculated in `src/pipeline/pipe_ev_regional_consumption.py`
2. the temporal disaggregation (historical and future) is calculated in `src/pipeline/pipe_ev_temporal.py`



For a detailed description of the most important pipelines, see [src/pipeline/README.md](src/pipeline/README.md).
In [data/README.md](data/README.md) you can find a detailed description of the input data.
For a description of the config files, see [src/configs/README.md](src/configs/README.md).



## Installation
### Install VENV
`pip3 install virtualenv`

### Create and activate VENV
`python3 -m venv env`
`source env/bin/activate`

### Install Dependencies
`pip install -r requirements.txt`

### Freeze Dependencies
`pip freeze > requirements.txt`

### View Dependencies
`pip3 list`
or look in the `requirements.txt`







## Dictionary
| Term                    | Description                                                                                                           | German Translation                          |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------|----------------------------------|
| `sector`                | The sectors Commerce, Transport and Services (CTS) or industry | Sektoren |
| `industry_sector`       | The 88 unique economic sectors included in the CTS and industry domains.                                             | Wirtschaftszweige               |
| `industry_sector_ranges`| Groups of `industry_sector`, comprising 48 unique aggregates.                                                        | Wirtschaftsbereiche             |
| `energy_carriers`       | The energy carriers used in the model: `power` (electricity), `gas` (natural gas), `hydrogen`, `petrol_products` or `petrol`. ("source" in the old codebase) | Energieträger                   |
| `regional_id`           | The 400 unique regional identifiers for Germany, corresponding to NUTS-3 regions.                                   | Allgemeiner Gemeindeschlüssel (AGS) |
| `activity_drivers`      | Expected economic development effects on energy demand per `industry_sector`.                                        | Mengeneffekt / Mengentreiber    |
| `efficiency_effect`     | Expected technological efficiency improvements affecting energy demand per application.                              | Effizienzeffekt                 |
| `fuel_switch`           | Transition from fossil fuels (natural gas and petroleum products) to hydrogen and electricity to achieve Germany’s greenhouse gas neutrality by 2045. | Energieträgerwechsel            |





## Concepts
### Industry Sectors /Produktions Bereiche /Wirtschaftszweige (WZ 2008)
- 48 industry sector groups
- 88 industry sectors (1-99 without 4, 34, 40, 44, 48, 54, 57, 67, 76, 83, 89)
- 29 industry sectors are building the CTS sector (5-33), the other 58 are building the industry sector
- can also called economic sectors

### Regional IDs
Classification of german regions
- 400 regional_ids for 2021 following the AGS_LK
- additional information can be found in the Gemeindeverzeichnis-Informationssystem GV-ISys https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/Gemeindeverzeichnis/_inhalt.html
- can be translated to NUTS-3 codes





## Others
- depreciated codebase is available at https://github.com/DemandRegioTeam/disaggregator/tree/Dev_Applications
