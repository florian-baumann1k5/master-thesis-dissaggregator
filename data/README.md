# Data
Datasources and explanations about the input data

### Application Dissaggregation Factors:
Self generated application shares per industry sector based on literature from AGEB and VDI.
See the underlying papers of this code for the methodology. 
- data/raw/dimensionless/decomposition_factors_process_heat.csv
- data/raw/dimensionless/decompositionfactors_petrol_general_2023.csv
- data/raw/dimensionless/decomposition_factors.xlsx


### data/raw/dimensionless:
- decomposition factors for application wise disaggregation
- /ugr_2000to2020.csv
    - Downloaded on 21.03.2025 from Genisis DB https://www-genesis.destatis.de/datenbank/online/statistic/85121/table/85121-0002/.
    - Selected the years 2000 to 2020 (newest data availible)
    - downloaded as flat csv without "Qualitätskennzeichen einbeziehen"
    - Energieverbrauch pro WZ in TJ
- energiebilanzen for calculating the self consumption


### data/raw/electric_vehicles
- charging profiles for temporal disaggregation
- KBA dataset as input for the KBA approach (historical) and for the regional disaggregation (UGR approach)
- shares of commercial EVs by regional_id
- average km driven by EVs 
- the projected number of EVs for the KBA_2 approach
- the UGR data on electricity usage in private households for transportation


### data/raw/heat
- COP parameters for Heat Pump
- Fuel switch keys of how the gas and petrol consumption is switched to electricity and hydrogen


### data/raw/regional
- changes in the AGS definition over the time
- shapefile of the NUTS-3 regions

### data/raw/temporal
- Tempoerature data
- Gas load profiles 
- Electricity load profiles
- Activity drivers (Mengeneffekt) for economic sectors
- Efficency effects per application
