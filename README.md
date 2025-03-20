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




--------------------------------
Piplelines are calling data_processing funcitons



data_processing:
(input df and output df)
- making changes to dfs

Pipelines:
(combining data_processing functions -> highlevel)
- consumption brach and district (= just a mapper of )
- disagg_temporal_applications (is is a highlevel function of all the other dissagg fucntiuon (see miro))