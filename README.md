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
- WZ = industries
- energy carriers = power, gas, hydrogen, petrol_products