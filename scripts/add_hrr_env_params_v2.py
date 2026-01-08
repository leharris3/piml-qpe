import zarr
import s3fs
import pandas as pd
import xarray as xr
import numpy as np

from pathlib import Path
from datetime import datetime
from glob import glob
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

from scipy.spatial import cKDTree


# for projecting zarr -> lat/lon
url = "s3://hrrrzarr/sfc/20210601/20210601_00z_anl.zarr"
fs = s3fs.S3FileSystem(anon=True)

# used for coordinate projection
chunk_index = xr.open_zarr(s3fs.S3Map("s3://hrrrzarr/grid/HRRR_chunk_index.zarr", s3=fs))

# --- load dataframe ---
# Avoid reading then dropping "Unnamed: 0"
df1 = pd.read_csv(
    "/playpen-ssd/levi/ccrfcd-gauge-grids/data/2021-01-01_2025-07-25_gt_p1.csv",
    index_col=0,
)
df2 = pd.read_csv(
    "/playpen-ssd/levi/ccrfcd-gauge-grids/data/2021-01-01_2025-07-25_gt_p2.csv",
    index_col=0,
)
df  = pd.concat([df1, df2], axis=0, ignore_index=True)


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

# Faster membership test than scanning VARS_OF_INTEREST for every zarr file
VARS_SET = {(v["level"], v["name"]) for v in VARS_OF_INTEREST}

HRRR_ENV_DIR = "/playpen-ssd/levi/ccrfcd-gauge-grids/data/hrrr-env"

# TZ assumed UTC
dt_fp_dict = {datetime.strptime(Path(fp).name, "%Y%m%d_%Hz_anl"): fp for fp in glob(f"{HRRR_ENV_DIR}/*")}


def get_hrrr_dir_path(dt: datetime) -> str | None:
    # truncate precision < hour
    truc_dt = datetime(year=dt.year, month=dt.month, day=dt.day, hour=dt.hour)
    return dt_fp_dict.get(truc_dt)


# Cache env var listings per HRRR directory so we don't glob/open metadata per row
_env_vars_cache: dict[str, list[dict]] = {}


def get_env_vars(hrrr_dir: str) -> list[dict]:
    """
    Return a list of dicts like:
        [{"level": ..., "var_name": ..., "var_zarr": zarr.Array}, ...]
    """
    if hrrr_dir in _env_vars_cache:
        return _env_vars_cache[hrrr_dir]

    evd = []
    env_var_zarrs = glob(f"{hrrr_dir}/*/*/*/*/.zarray")

    for fp in env_var_zarrs:
        p = Path(fp)
        env_var_zarr_fp = p.parent
        env_var_name    = p.parent.name
        level_name      = p.parent.parent.name

        if (level_name, env_var_name) in VARS_SET:
            evd.append({
                "level": level_name,
                "var_name": env_var_name,
                "var_zarr": zarr.open_array(env_var_zarr_fp, mode="r"),
            })

    _env_vars_cache[hrrr_dir] = evd
    return evd


def build_latlon_index(chunk_index):
    """
    Build a nearest-neighbor index for the HRRR grid.
    Returns (tree, grid_shape, lons_0_360_flag).
    """
    # Use .values to avoid extra xarray wrapping work
    lats = np.asarray(chunk_index.latitude.values)
    lons = np.asarray(chunk_index.longitude.values)

    lons_0_360 = (np.nanmin(lons) >= 0) and (np.nanmax(lons) <= 360)
    if lons_0_360:
        lons = np.mod(lons, 360.0)

    pts = np.column_stack([lats.ravel(), lons.ravel()])
    tree = cKDTree(pts)
    return tree, lats.shape, lons_0_360


def latlon_to_iyix(lat, lon, tree, grid_shape, lons_0_360=True):
    """
    Convert (lat, lon) to nearest (iy, ix) once, then reuse for all variables.
    """
    if lons_0_360:
        lon = lon % 360.0
    dist, flat_idx = tree.query([lat, lon], k=1)
    iy, ix = np.unravel_index(flat_idx, grid_shape)
    return int(iy), int(ix), float(dist)


# Build index once
tree, grid_shape, lons_0_360 = build_latlon_index(chunk_index)


def proc_row(i, row):
    vals = row

    # Much cheaper than split/join, assuming start_datetime_utc begins with "YYYY-MM-DD HH"
    dt = datetime.strptime(vals.start_datetime_utc[:13], "%Y-%m-%d %H")

    hrrr_dir = get_hrrr_dir_path(dt)
    if hrrr_dir is None:
        return i, {}

    lat, lon = vals.lat, vals.lon
    env_vars = get_env_vars(hrrr_dir)
    if not env_vars:
        return i, {}

    # Compute nearest grid point ONCE per row
    iy, ix, _dist = latlon_to_iyix(lat, lon, tree, grid_shape, lons_0_360=lons_0_360)

    row_dict = {}
    # Read a single element from each zarr array (no full-array materialization)
    for var in env_vars:
        comb_name = var["level"] + "_" + var["var_name"]
        row_dict[comb_name] = float(var["var_zarr"][iy, ix])

    return i, row_dict


all_rows = {}

import json

# Threading across rows only (avoid nested thread pools)
with ThreadPoolExecutor() as ex:
    completed = 0
    for i, row_dict in tqdm(
        ex.map(proc_row, range(len(df)), df.itertuples(index=False)),
        total=len(df)
    ):
        all_rows[i] = row_dict
        completed += 1

        # Write less often and based on completed count (not i, since results may be out of order)
        if completed % 1000 == 0:
            with open("scripts/hrrr_env_rows.json", "w") as f:
                json.dump(all_rows, f)

with open("scripts/hrrr_env_rows.json", "w") as f:
    json.dump(all_rows, f)