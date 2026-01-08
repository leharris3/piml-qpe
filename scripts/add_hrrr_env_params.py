import zarr
import timeit
import s3fs
import pandas as pd
import xarray as xr

from pathlib import Path
from datetime import datetime, timezone
from glob import glob


# for projecting zarr -> lat/lon
url = "s3://hrrrzarr/sfc/20210601/20210601_00z_anl.zarr"
fs = s3fs.S3FileSystem(anon=True)

# used for coordinate projection
chunk_index = xr.open_zarr(s3fs.S3Map("s3://hrrrzarr/grid/HRRR_chunk_index.zarr", s3=fs))

# --- load dataframe ---
df1 = pd.read_csv("/playpen-ssd/levi/ccrfcd-gauge-grids/data/2021-01-01_2025-07-25_gt_p1.csv")
df2 = pd.read_csv("/playpen-ssd/levi/ccrfcd-gauge-grids/data/2021-01-01_2025-07-25_gt_p2.csv")
df  = pd.concat([df1, df2], axis=0).drop("Unnamed: 0", axis=1)


VARS_OF_INTEREST = [
    {"name": "DPT", "level": "2m_above_ground", "store": "sfc"},
    {"name": "PWAT", "level": "entire_atmosphere_single_layer", "store": "sfc"},
    {"name": "HGT", "level": "level_of_adiabatic_condensation_from_sfc", "store": "sfc"},
    {"name": "HGT", "level": "highest_tropospheric_freezing_level", "store": "sfc"},
    {"name": "APCP", "level": "surface", "store": "sfc"},
    {"name": "DPT", "level": "925mb", "store": "sfc"},
    {"name": "DPT", "level": "850mb", "store": "sfc"},
    {"name": "DPT", "level": "700mb", "store": "sfc"},
    {"name": "DPT", "level": "500mb", "store": "sfc"},
    {"name": "UGRD", "level": "850mb", "store": "sfc"},
    {"name": "VGRD", "level": "850mb", "store": "sfc"},
    {"name": "UGRD", "level": "700mb", "store": "sfc"},
    {"name": "VGRD", "level": "700mb", "store": "sfc"},
    {"name": "TMP", "level": "2m_above_ground", "store": "sfc"},
    {"name": "PRES", "level": "surface", "store": "sfc"},
    {"name": "HGT", "level": "700mb", "store": "sfc"},
    {"name": "HGT", "level": "850mb", "store": "sfc"},
    {"name": "TMP", "level": "700mb", "store": "sfc"},
    {"name": "TMP", "level": "850mb", "store": "sfc"},
    {"name": "TMP", "level": "500mb", "store": "sfc"},
    {"name": "SPFH", "level": "1000mb", "store": "prs"},
    {"name": "SPFH", "level": "925mb",  "store": "prs"},
    {"name": "SPFH", "level": "850mb",  "store": "prs"},
    {"name": "SPFH", "level": "700mb",  "store": "prs"},
    {"name": "RH",   "level": "925mb",  "store": "prs"},
    {"name": "RH",   "level": "850mb",  "store": "prs"},
    {"name": "RH",   "level": "700mb",  "store": "prs"},
]

HRRR_ENV_DIR = "/playpen-ssd/levi/ccrfcd-gauge-grids/data/hrrr-env"

# TZ assumed UTC
dt_fp_dict = {datetime.strptime(Path(fp).name, "%Y%m%d_%Hz_anl") : fp for fp in glob(f"{HRRR_ENV_DIR}/*")}


def get_hrrr_dir_path(dt: datetime) -> str | None:
    # truncate precision < hour
    y, m, d, h = dt.year, dt.month, dt.day, dt.hour
    truc_dt = datetime(year=y, month=m, day=d, hour=h)
    # return fp if in dict
    if truc_dt in dt_fp_dict: return dt_fp_dict[truc_dt]
    else: return None


def get_vertical_levels(hrrr_dir: str) -> list:
    return [Path(fp).name for fp in glob(f"{hrrr_dir}/*")]


def get_env_vars(hrrr_dir: str) -> dict:
    """
    Given a path to a HRRR dir (i.e., valid date/hr),

    Return
    ---
    ```
        [{
            "var_name": ...,
            "level": ...,
            "zgroup": ...,
            "zarr": ...,
        }]
    ```
    """

    evd = []
    env_var_zarrs = glob(f"{hrrr_dir}/*/*/*/*/.zarray")

    for fp in env_var_zarrs:

        p = Path(fp)

        # path to zarr dir for an atmospheric var
        env_var_zarr_fp  = p.parent
        env_var_group_fp = p.parent.parent.parent
        env_var_name     = p.parent.name
        level_name       = p.parent.parent.name

        for var_of_int in VARS_OF_INTEREST:
            lvl = var_of_int['level']
            nme = var_of_int['name']
            if lvl == level_name and nme == env_var_name:
                evd.append({
                    "level": level_name,
                    "var_name": env_var_name,
                    "var_group": zarr.open_group(env_var_group_fp),
                    "var_zarr": zarr.open_array(env_var_zarr_fp),
                })
                break

    return evd


dir_fp = get_hrrr_dir_path(datetime(year=2021, month=1, day=24, hour=0))
env_var_d = get_env_vars(dir_fp)


import numpy as np
from scipy.spatial import cKDTree


def build_latlon_index(chunk_index):
    """
    Build a nearest-neighbor index for the HRRR grid.
    Returns (tree, grid_shape, lons_0_360_flag).
    """
    lats = np.asarray(chunk_index.latitude)
    lons = np.asarray(chunk_index.longitude)

    # Normalize lon handling (your notebook uses 0..360)
    lons_0_360 = (np.nanmin(lons) >= 0) and (np.nanmax(lons) <= 360)

    if lons_0_360:
        lons = np.mod(lons, 360.0)  # keep 0..360
    else:
        # keep -180..180 form; still OK, just be consistent
        pass

    pts = np.column_stack([lats.ravel(), lons.ravel()])

    if cKDTree is None:
        raise ImportError("scipy is required for the fast KDTree approach (pip install scipy).")

    tree = cKDTree(pts)
    return tree, lats.shape, lons_0_360


def value_at_latlon(lat, lon, arr, tree, grid_shape, lons_0_360=True):
    """
    Nearest-neighbor value from arr at (lat, lon).
    Returns (value, (iy, ix), distance_degrees).
    """
    
    # if lon in range: 0-360 -> -180-180
    if lons_0_360:
        lon = lon % 360.0

    dist, flat_idx = tree.query([lat, lon], k=1)
    iy, ix = np.unravel_index(flat_idx, grid_shape)
    return float(arr[iy, ix]), (int(iy), int(ix)), float(dist)


# Build index once
tree, grid_shape, lons_0_360 = build_latlon_index(chunk_index)


def proc_var(var, dt, lat, lon):

    # unique level, name ID
    comb_name = var["level"] + "_" + var["var_name"]
    
    if comb_name not in zarr_cache[dt]:

        # load zarr into mem; store in cache
        za = var['var_zarr'][:]
        zarr_cache[dt][comb_name] = {
            "zarr": za
        }

    if "val" not in zarr_cache[dt][comb_name]:

        val, (iy, ix), dist = value_at_latlon(lat, lon, za, tree, grid_shape, lons_0_360=lons_0_360)
        zarr_cache[dt][comb_name]["val"] = val

    # load from cache
    val = zarr_cache[dt][comb_name]["val"]

    return (comb_name, val)


def proc_row(i, row):
    
    vals     = row
    dt_str   = " ".join(vals.start_datetime_utc.split(":")[:-3])
    dt       = datetime.strptime(dt_str, "%Y-%m-%d %H")

    # lookup table for caching zarrs
    if dt not in zarr_cache: zarr_cache[dt] = {}
    
    # get path to corresponding hrrr zarr dir
    hrrr_dir = get_hrrr_dir_path(dt)
    lat, lon = vals.lat, vals.lon

    env_vars = get_env_vars(hrrr_dir)
    assert len(env_vars) > 0

    # print("---------------------")
    # print(f"Date: {dt}")

    row_dict = {}

    with ThreadPoolExecutor() as ex:

        procs = [ex.submit(proc_var, var, dt, lat, lon) for var in env_vars]
        for res in as_completed(procs):
            
            result = res.result()
            name, val = result
            row_dict[name] = val

    return i, row_dict


from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# for each row, start_datetime
    # hrrr_dir      <-- get hrrr_dir        // find the corresponding HRRR dir for this hour if it exists
    # env_var_zarrs <-- get_zarrs(hrrr_dir) // list of zarrs
    
    # [] // name, val
    # for za in env_var_zarrs
        # lat, lon     <-- row.lat, row.lon
        # val_at_gauge <-- get_value_at(zarr, lat, lon)
        # [].append((za.name, val_at_gauge))


# {datetime: {var: {zarr, location: val@loc}}
zarr_cache = {}
all_rows   = {}


import json


with ThreadPoolExecutor() as ex:

    procs = []
    for i, row in tqdm(enumerate(df.itertuples()), total=len(df)):
        procs.append(ex.submit(proc_row, i, row))

    for proc in tqdm(as_completed(procs), total=len(procs)):
        i, row_dict = proc.result()
        all_rows[i] = row_dict
        if i % 100 == 0: 
            with open("scripts/hrrr_env_rows.json", "w") as f:
                json.dump(all_rows, f)

with open("scripts/hrrr_env_rows.json", "w") as f: json.dump(all_rows, f)