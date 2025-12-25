# **PI-ML QPE for the western CONUS**
---
> *NOTE: this is work in-progress.*

[![Hugging Face Dataset](https://img.shields.io/badge/Hugging%20Face-Dataset-FFD21E?logo=huggingface&logoColor)](https://huggingface.co/datasets/leharris3/ccrfcd-mrms-hrrr-env-2021-2025)

### Dataset

|                                             variable |   units    |                                            source                                             | notes                                 |
| ---------------------------------------------------: | :--------: | :-------------------------------------------------------------------------------------------: | :------------------------------------ |
|                                          `gauge_idx` |    –––     |                       [CCRFCD](https://gustfront.ccrfcd.org/gaugemap/)                        |                                       |
|                                 `start_datetime_utc` | `datetime` |                                              –––                                              |                                       |
|                                   `end_datetime_utc` | `datetime` |                                              –––                                              |                                       |
|                                       `gauge_acc_in` |  inch/hr   |                       [CCRFCD](https://gustfront.ccrfcd.org/gaugemap/)                        | rain gauge 1H accumulation            |
|                                    `mrms_q3evap_qpe` |  inch/hr   |                     [MRMS](https://registry.opendata.aws/noaa-mrms-pds/)                      | MRMS opperational 1H radar-only QPE   |
|                                                `lat` |  degrees   |                       [CCRFCD](https://gustfront.ccrfcd.org/gaugemap/)                        | rain-gauge latitude                   |
|                                                `lon` |  degrees   |                       [CCRFCD](https://gustfront.ccrfcd.org/gaugemap/)                        | rain-gauge longitude                  |
|                                         `700mb_UGRD` |    m/s     | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  | `u` component of wind                 |
|                                         `700mb_VGRD` |    m/s     | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  | `v` component of wind                 |
|                                  `850_700_mean_wind` |    m/s     | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* |                                       |
|                                       `surface_PRES` |     Pa     | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                          `850mb_HGT` |    gpm     | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                          `700mb_HGT` |    gpm     | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|      `highest_tropospheric_`<br>`freezing_level_HGT` |     m      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
| `level_of_adiabatic_`<br>`condensation_from_sfc_HGT` |    gpm     | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                `2m_above_ground_TMP` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                          `850mb_TMP` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                          `700mb_TMP` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                          `500mb_TMP` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                `2m_above_ground_DPT` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                          `925mb_DPT` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                          `850mb_DPT` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                          `700mb_DPT` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                          `500mb_DPT` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                    `925mb_700mb_DPT` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* |                                       |
|                                    `850mb_700mb_DPT` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* |                                       |
|                                    `850mb_500mb_DPT` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* |                                       |
|                                    `surface_theta_e` |     K      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* |                                       |
|               `lowest_100mb_mean_`<br>`mixing_ratio` |   ratio    | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* |                                       |
|                                         `LCL_height` |     m      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* |                                       |
|                                         `LCL_LFC_RH` |     %      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* | relative humidity between LCL and LFC |
|                                           `0-3km_RH` |     %      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* | 0-3 km (mean) relative humidity       |
|                                           `0-5km_RH` |     %      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* | 0-5 km (mean) relative humidity       |
|                                              `DCAPE` |    J/kg    | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* |                                       |
|                               `low_level_lapse_rate` |    K/km    | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* |                                       |
|                              `3hr_lapse_rate_change` |    K/km    | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* |                                       |
|                                         `sfc_850_pw` |     mm     | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* | pwat, surface to 850mb                |
|                                         `sfc_700_pw` |     mm     | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)* | pwat, surface to 700mb                |
|          `entire_atmosphere_`<br>`single_layer_PWAT` |   kg/m^2   | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |
|                                               `CWCD` |     m      | [HRRR Zarr](https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/zarr_variables.html)  |                                       |

\**Fields derived from Zarr HRRR*