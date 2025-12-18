import zarr
import pandas as pd
from pathlib import Path
from datetime import datetime
from glob import glob


ALL_EVENTS_DF_P1  = "data/2021-01-01_2025-07-25_gt_p1.csv"
ALL_EVENTS_DF_P2  = "data/2021-01-01_2025-07-25_gt_p2.csv"
HRRR_ENV_DATA_DIR = "data/hrrr-env"


def load_cc_mrms_df():

    # load in the dataset
    df_p1 = pd.read_csv(ALL_EVENTS_DF_P1)[['gauge_idx', 'start_datetime_utc', 'end_datetime_utc', 'gauge_acc_in', 'mrms_q3evap_qpe', 'lat', 'lon']]
    df_p2 = pd.read_csv(ALL_EVENTS_DF_P2)[['gauge_idx', 'start_datetime_utc', 'end_datetime_utc', 'gauge_acc_in', 'mrms_q3evap_qpe', 'lat', 'lon']]
    df    = pd.concat([df_p1, df_p2], axis=0)

    # convert -> datetime objects
    df['start_datetime_utc'] = pd.to_datetime(df['start_datetime_utc'], errors='coerce', utc=True)
    df['end_datetime_utc']   = pd.to_datetime(df['end_datetime_utc']  , errors='coerce', utc=True)
    return df


class CC_MRMS_HRRR_Dataset:
    """
    A wrapper for CCRFCD rain guage + MRMS + HRRR reanalysis data.
    """

    def __init__(self):
        
        self.cc_mrms_df = load_cc_mrms_df()
        glob(f"/playpen-ssd/levi/ccrfcd-gauge-grids/data/hrrr-env/20210123_23z_anl/*/.zgroup", recursive=False)
        breakpoint()


    def get_data(self, dt: datetime) -> dict:

        # 
        return {   }


if __name__ == "__main__":
    ds = CC_MRMS_HRRR_Dataset(); breakpoint()