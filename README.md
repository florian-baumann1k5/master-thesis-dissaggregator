# master-thesis-dissaggregator


# DemandRegio Disaggregator

The DamendRegio project aims to disaggregate the final energy cosnumption of the sectors
- Industry
- Commercial, Trade and Services (CTS)
- Private Household
into a high spacial and temporal resolution.

This project includes:
- The energy carriers: power, gas, petrol products for the industry and CTS sector
- Power consumption of the private households for electric vehicles


## Structure of the Disaggregator
### General
The Disaggregator is structured in the following way:
- src/pipeline/*: Contains the main functions of the Disaggregator
- src/data_processing/*: Contains the data manipulation functions
- src/configs/*: Contains the configuration files and mappings
- data/output/*: Contains the output data
- data/raw/*: Contains the input data
- src/data_access/*: Contains the data access functions (API client and local file reader)
- src/utils/*: Contains the utility and execution functions




### Consumption
This files contain the functionalitie to disaggregate the consumption on a level of industry sectors and regional_ids.

### Applications
Contains the functionalities to disaggregate the consumption on a level of applications.

### Temporal
Contains the functionalities to disaggregate the consumption on a level of temporal resolution.

### Heat
Contains the functionalities to transfere gas and petrol consumption to hydrogen and electricity consumption (fuel switch)

### EV
Contains two approaches to disaggregate and project the power consumption of electric vehicles in private households. 




## Installation
### Install VENV
'pip3 install virtualenv'

### Create and activate VENV
'python3 -m venv env'
'source env/bin/activate'

### Install Dependencies
'pip install -r requirements.txt'

### Freeze Dependencies
'pip freeze > requirements.txt'

### View Dependencies
'pip3 list'
or look in the requirements.txt







## Dictionary
- WZ                                                    = industry_sector [88 unique sectors]
- WZs e.g. 37-39                                        = industry_sector groups [48 unique groups]
- energy carriers                                       = power, gas, hydrogen, petrol_products
- ags_lk = Allgemeneiner Gemeindeschlüssel Landkreise   = regional_id [400 unique regions for 2021 following]
- activity_drivers                                      = Mengentreiber (DISS 4.5.1)
- "source" in the old code                              = energy_carrier



## Concepts
### Industry Sectors /Produktions Bereiche /Wirtschaftszweige (2008)

- 48 industry sector groups
- 88 industry sectors (1-99 without 4, 34, 40, 44, 48, 54, 57, 67, 76, 83, 89)
- 29 industry sectors are building the CTS sector (5-33), the other 58 are building the industry sector

### Regional IDs
Classification of german regions
- 400 regional_ids for 2021 following the AGS_LK
- additional information can be found in the Gemeindeverzeichnis-Informationssystem GV-ISys https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/Gemeindeverzeichnis/_inhalt.html
- can be translated to NUTS3 codes





## Others
- using google Docstings
- depreciated codebase https://github.com/DemandRegioTeam/disaggregator


## License
Current version of software written and maintained by Paul A. Verwiebe (TUB)

Original version of software written by Fabian P. Gotzens (FZJ), Paul A. Verwiebe (TUB), Maike Held (TUB), 2019/20.

disaggregator is released as free software under the [GPLv3](http://www.gnu.org/licenses/gpl-3.0.en.html), see [LICENSE](LICENSE) for further information.