# **PI-ML QPE for the western CONUS**
---
> *NOTE: this is work in-progress.*

[![Hugging Face Dataset](https://img.shields.io/badge/Hugging%20Face-Dataset-FFD21E?logo=huggingface&logoColor)](https://huggingface.co/datasets/leharris3/ccrfcd-mrms-hrrr-env-2021-2025)

### Dataset

| variable | units | notes |
| ---:    | :---: | :---: |
| `gauge_idx`          | ---       | gauge identifier |
| `start_datetime_utc` | `datetime`| |
| `end_datetime_utc`   | `datetime`| |
| `gauge_acc_in`       | inch/hr   | CCRFCD rain gauge 1H accumulation |
| `mrms_q3evap_qpe`    | inch/hr   | MRMS Q3 evaporation-adjusted QPE |
| `lat`                | degrees   | latitude |
| `lon`                | degrees   | longitude |
| `700mb_UGRD`         | m/s       | `u` component of wind at 700mb |
| `700mb_VGRD`         | m/s       | `v` component of wind at 700mb |
| `850_700_mean_wind`  | m/s       | mean wind speed in 850-700mb layer |
| `850mb_DPT`          | K         | dewpoint temperature at 850mb |
| `850mb_500mb_DPT`    | K         | dewpoint (mean) |
| `LCL_LFC_RH`         | %         | relative humidity between LCL and LFC |
| `0-3km_RH`           | %         | 0-3 km (mean) relative humidity |
| `0-5km_RH`           | %         | 0-5 km (mean) relative humidity |
| `DCAPE`              | J/kg      | downdraft convective available potential energy |
| `low_level_lapse_rate` | K/km    | low-level temperature lapse rate |
| `3hr_lapse_rate_change` | K/km   | 3-hour change in lapse rate |
| `sfc_850_pw`         | mm        | precipitable water, surface to 850mb |
| `sfc_700_pw`         | mm        | precipitable water, surface to 700mb |
