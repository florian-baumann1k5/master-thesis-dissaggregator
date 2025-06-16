# Disaggregator Pipelines

In this file the main pipelines of the Disaggregator are described.
Addiionally other important files and functions are described.

## Consumption: `src/pipeline/pipe_consumption.py`:
This files contain the functionalitie to disaggregate the consumption on a level of industry sectors and regional_ids.
- `get_consumption_data()`: Get the consumption data for a specific year and specific energy carrier.
- `get_consumption_data_per_indsutry_sector_energy_carrier()`: Get the consumption data for a specific year and specific energy carrier for a specific industry sector (CTS or industry).
- `get_consumption_data_historical_and_future()`: Get the consumption data for a specific year and specific energy carrier for a specific industry sector (CTS or industry).



## Applications: `src/pipeline/pipe_applications.py`:
Contains the functionalities to disaggregate the consumption on a level of applications.

`disagg_applications_efficiency_factor()`: Dissaggregate the consumption data based on the applications and apply efficiency enhancement factors. The function for the effect is in `src/data_processing/effects.py` and called `apply_efficiency_factor()`.


## Temporal: `src/pipeline/pipe_temporal.py`:
Contains the functionalities to disaggregate the consumption on a level of temporal resolution.
`disaggregate_temporal(...)`: Disaggregates the results from the application pipeline to a temporal resolution. Differentiating between the different sectors and energy carriers.


## Heat: `src/pipeline/pipe_heat.py`:
Contains the functionalities to transfere gas and petrol consumption to hydrogen and electricity consumption (fuel switch).
`sector_fuel_switch_fom_gas_petrol()`: Calculates the share of gas and petrol that is needed to be switched to hydrogen or electricity.
`temporal_elec_load_from_fuel_switch()`: Calculates the electricity demand that is needed to replace the gas or petrol consumption.
`temporal_hydrogen_load_from_fuel_switch()`: Calculates the hydrogen demand that is needed to replace the gas consumption.





## EV regional consumption: `src/pipeline/pipe_ev_regional_consumption.py`:
Contains two approaches to disaggregate and project the power consumption of electric vehicles in private households on a level of 400 regional_ids.
The KBA approaches (also referred as s1 and s2) are calculating the consumption based on: vehicle stock data * average km driven by EVs * average mwh per km. For the historical calculations both KBA approaches using the same data.The KBA_1 (s1) approach uses the normative goal (15mio EVs by 2030 and only EVs by 2045) and the KBA_2 (s2) approach uses the projected number of EVs from literature to estimate the future vehicle stock.

## EV temporal consumption: `src/pipeline/pipe_ev_temporal.py`:
Contains the functionalities to disaggregate the consumption on a level of temporal resolution.
KBA approaches are using the charging profiles of all locations (home, work, public), since the EVs can be charged at all locations. 
The UGR approach is using the charging profiles of the home location since it already only contains the electricity demand in private households.





## Others:
We work with datasets from different time periods, and while using regional_ids is helpful, these identifiers can change over time. To address this, we've created mapping files located at `data/raw/regional/ags_lk_changes` to normalize the regional_ids.
For more details, refer to the `data/raw/regional/ags_lk_changes/README.md` file.






