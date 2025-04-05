# master-thesis-dissaggregator

## Virtual Envirenment
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


## Pre-Installation
### Download the newest version of UGR from Genisis DB https://www-genesis.destatis.de/datenbank/online/statistic/85121/table/85121-0002/ as flat csv, name it "ugr_<start_year>_<end_year>.csv"

--------------------------------
Piplelines are calling data_processing funcitons



data_processing:
(input df and output df)
- making changes to dfs

Pipelines:
(combining data_processing functions -> highlevel)
- consumption brach and district (= just a mapper of )
- disagg_temporal_applications (is is a highlevel function of all the other dissagg fucntiuon (see miro))


## Others
- using google Docstings
- 

## Dict
- WZ                                                    = industry_sector [88 unique sectors]
- WZs e.g. 37-39                                        = industry_sector groups [48 unique groups]
- energy carriers                                       = power, gas, hydrogen, petrol_products
- ags_lk = Allgemeneiner Gemeindeschlüssel Landkreise   = regional_id [400 unique regions for 2021 following]
- activity_drivers = Mengentreiber (DISS 4.5.1)
- "source" in the old code = energy_carriere


## Industry Sectors /ProduktionsBereiche
- 48 industry sector groups
- 88 industry sectors (1-99 without 4, 34, 40, 44, 48, 54, 57, 67, 76, 83, 89)