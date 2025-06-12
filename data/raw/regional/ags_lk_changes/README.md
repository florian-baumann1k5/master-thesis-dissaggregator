# Generals
Files are showing the changes in the "Landkeis" or "allgemeiner Gemeindeschlüssel (AGS)" definitions to be able to normalize the "allgemeiner Gemeindeschlüssel (AGS)" to the 2024 areas.

### Changes to the regional_ids over time
`data/raw/regional/ags_lk_changes/changes_XXXtoXX.csv`:
List the changes in the regional_ids over time. The regional_ids are only merged, never devided.
Changes csv files are created from the source: https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/Gemeindeverzeichnis/Namens-Grenz-Aenderung/namens-grenz-aenderung.html

Here a table of the unique regional_id keys over time:
| Time Period   | Unique regional_id Keys |
|---------------|--------------------|
| 2001–2006     | 439                |
| 2007          | 429                |
| 2008          | 413                |
| 2009–2010     | 412                |
| 2011–2015     | 402                |
| 2016–2020     | 401                |
| 2021–2024     | 400                |



# Total number of unique regional_id keys (2024)
`data/raw/regional/ags_lk_changes/landkreise_2023.csv`: Describes the latest state of the 'Landkreise'. The last change to this list happend in 2021.
final ags_lk/ regional_id/ NUTS-3 list downloaded from: 
https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/Gemeindeverzeichnis/Administrativ/Archiv/GVAuszugJ/31122023_Auszug_GV.html
This file also provodes a translation of the regional_ids to the NUTS-3 codes.