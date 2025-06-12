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

`disagg_applications_efficiency_factor()`: Dissaggregate the consumption data based on the applications and apply efficiency enhancement factors. The function for the effect is in `src/data_processing/effects.py` and called 'apply_efficiency_factor()'.


## Temporal: `src/pipeline/pipe_temporal.py`:
Contains the functionalities to disaggregate the consumption on a level of temporal resolution.



## Heat: `src/pipeline/pipe_heat.py`:
Contains the functionalities to transfere gas and petrol consumption to hydrogen and electricity consumption (fuel switch)



## EV: `src/pipeline/pipe_ev.py`:
Contains two approaches to disaggregate and project the power consumption of electric vehicles in private households. 



## Others:
We work with datasets from different time periods, and while using regional_ids is helpful, these identifiers can change over time. To address this, we've created mapping files located at `data/raw/regional/ags_lk_changes` to normalize the regional_ids.
For more details, refer to the `data/raw/regional/ags_lk_changes/README.md` file.






