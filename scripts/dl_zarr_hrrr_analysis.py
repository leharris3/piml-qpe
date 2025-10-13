import os
import xarray
# a fast xarray-like library for sci-ml apps
import zarr
import pandas as pd

from glob import glob
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from src.utils.mrms.mrms import MRMSAWSS3Client


CLIENT = MRMSAWSS3Client()
# these are the zarr chunks we keep
# roughly corresponing to the VEF CWA
# https://mesowest.utah.edu/html/hrrr/zarr_documentation/html/python_data_loading.html
CHUNKS     = ["4.1", "4.2", "3.1", "3.2", "2.2"]
OUTDIR     = "data/hrrr-env"
DATASET_FP = "data/events/2021-01-01_2025-07-25_all_events.csv"
BUCKET     = "s3://hrrrzarr/sfc/20200903/20200903_00z_anl.zarr/"


def download_zarr_and_del_extra_grids(url: str, out_dir: str) -> None:

    # 1. download all paths
    CLIENT.download(url, out_dir, recursive=True)

    # 2. remove extra grid cells
    # paths to grid-cells 
    fps = glob(f"{out_dir}/*/*/*/*/*")
    for fp in fps:
        grid_cell = fp.split("/")[-1]
        if grid_cell not in CHUNKS:
            os.remove(fp)


def download_dt(dt: datetime) -> None:

    # 1. create a subdir out if dne
    ymd = f"{dt.year}{dt.month:02d}{dt.day:02d}"
    subdir_name = f"{ymd}_{dt.hour:02d}z_anl"
    subdir_path = Path(OUTDIR) / Path(subdir_name)

    # NOTE: skip existing dirs, assume they are already processed
    if subdir_path.is_dir(): return
    
    os.makedirs(str(subdir_path), exist_ok=True)

    # 2. build path to a3 sub-bucket
    az_path = f"s3://hrrrzarr/sfc/{ymd}/{subdir_name}.zarr/"

    # 3. download recursively
    download_zarr_and_del_extra_grids(az_path, str(subdir_path))


def main() -> None:

    from tqdm import tqdm
    
    # 1. find all unique dts rounded to the nearest hour in our dataset
    df = pd.read_csv(DATASET_FP)
    
    # YYYY-MM-DD HH
    unique_dts_strs = list(set([str(s)[:-6] for s in df['start_time']]))
    unique_dts = [datetime.strptime(s, "%Y-%m-%d %H") for s in unique_dts_strs]

    # 2. loop through each dt, download data to it's own dir
    with ThreadPoolExecutor() as ex:
        futures = {ex.submit(download_dt, dt): dt for dt in unique_dts}
        for future in tqdm(as_completed(futures), total=len(futures)):
            dt = futures[future]
            try:
                _ = future.result()
            except Exception as e:
                print(f"[{dt}] failed: {e}")
            else:
                pass

if __name__ == "__main__": 
    main()