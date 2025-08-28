"""
# TODO:
---
We want to create 1x1km gridded products for CCRFCD rain gauges equivalent to
    1. MRMS_RadarOnly_QPE_1H
    2. MRMS_RadarOnly_QPE_3H

Also, want to create some plotting functionality to display gif-style loops.

A few problems to consider:
    1. MRMS temporal resolution is 2 min, CCRFCD data are 5 minutes
        - May consider creating a 10 minute gridded product
    2. What will be the dimensions of the actual gridded product?

keys:
    1. time
    2. lat/lon*
queries:
    1. [M, N] 2D array with ``mm`` accumulated precipitation values
"""

import logging
import numpy as np
import pandas as pd

from tqdm import tqdm
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed


class Location:

    def __init__(self, lat: float, lon: float):
        self.lat = lat
        self.lon = lon


class CCRFCDClient:

    _METADATA_FP    = "data/ccrfcd_rain_gauge_metadata.csv"
    _GAUGE_DATA_DIR = "data/7-23-25-scrape"

    # state of nevada
    _LAT_MIN = 34.751857
    _LAT_MAX = 37.103662
    _LON_MIN = -116.146925
    _LON_MAX = -113.792819

    # 0.1° ~ 10 km?
    # _DLAT = _DLON = 0.045
    _DLAT = _DLON = 0.02

    def __init__(self, ):

        self.metadata                            = pd.read_csv(CCRFCDClient._METADATA_FP)
        self.valid_station_ids                   = self.metadata[self.metadata['station_id'] > 0]['station_id'].astype(int).tolist()
        self.data_cache: Dict[int, pd.DataFrame] = {}

    def _get_gauge_df(self, gauge_id) -> pd.DataFrame | None:

        if gauge_id in self.data_cache:
            return self.data_cache[gauge_id]
        
        fp = Path(self._GAUGE_DATA_DIR) / f"gagedata_{gauge_id}.csv"
        if not fp.is_file():
            return None
        
        df = pd.read_csv(fp)
        df['datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
        df.set_index('datetime', inplace=True)
        self.data_cache[gauge_id] = df

        return df

    def _fetch_gauge_qpe(self, 
                         gauge_id: int, 
                         start_time: datetime, 
                         end_time: datetime
                         ) -> Tuple[Location, float, int]:
        """
        Returns
        --- 
        - Cumlative precipitation (QPE) for a clark county rain gauge between ``start_time`` and ``end_time``.
        """

        assert start_time < end_time, f"Error: expected `start_time` < `end_time`"

        df = self._get_gauge_df(gauge_id)
        if df is None:
            return (None, None, None)

        # one issue is that rain gauges occasionally reset (e.g., 3.0" -> 0.0")
        # these resets (and descending values, generally should be ignored)
        if 'delta' not in df.columns:

            delta_arr   = []
            values = list(df["Value"])

            for i, val in enumerate(values): 

                if i >= len(values) - 1: break
                curr_val = val
                prev_val = values[i + 1]
                delta    = curr_val - prev_val

                # handle gauge resets; negative values
                delta    = max(delta, 0.0)
                delta_arr.append(delta)

            delta_arr   = delta_arr + [0.0]
            df['delta'] = delta_arr

        # cumulative precip
        cum_precip = None

        # grab gauge location
        location_row = self.metadata[self.metadata['station_id'] == gauge_id]
        assert len(location_row) == 1, f"Error: no metadata available for `gauge_id`: {gauge_id}"
        row          = location_row.iloc[0]
        location     = Location(lat=float(row.lat), lon=float(row.lon))

        # get [start, end] bounds
        # TODO: fix, we should just index between start and end times
        # start_idx  = df.index.get_indexer([start_time], method='nearest')[0]
        # end_idx    = df.index.get_indexer([end_time],   method='nearest')[0]
        # cum_precip = df.iloc[end_idx:start_idx+1]['delta'].sum()

        cum_precip = df.loc[end_time:start_time]['delta'][:-1].sum()
        return location, float(cum_precip), gauge_id

    def _fetch_all_gauge_qpe(self, start_time: datetime, end_time: datetime, timezone="UTC", disable_tqdm=False) -> List[Dict]:
        """
        **Time Zone: UTC**
        Gather all cummulative precipitation values (in.) between bounds of [start_time, end_time]; inclusive.

        Returns
        ---
        ```python
        [{
            "station_id": int,
            "location": Location,
            "qpe": float
        }]
        ```
        """

        # UTC -> PDT
        if timezone == "UTC":
            start_time -= timedelta(hours=7)
            end_time   -= timedelta(hours=7)
        
        all_gauge_qpe = []
        
        with ThreadPoolExecutor() as executor:
            
            futures = {executor.submit(self._fetch_gauge_qpe, _id, start_time, end_time): _id for _id in self.valid_station_ids}
            for future in as_completed(futures):
                try:
                    res: Tuple[Optional[Location], Optional[float], Optional[int]] = future.result()
                    if res[0] is None: continue
                    all_gauge_qpe.append({
                        "station_id": res[2],
                        "lat": res[0].lat,
                        "lon": res[0].lon + 360,
                        "qpe": res[1],
                    })
                except Exception as e:
                    # HACK: silencing these errors for now
                    # print(e)
                    # print(f"Error fetching gauge id: ???")
                    pass
            
        # for _id in tqdm(self.valid_station_ids, total=len(self.valid_station_ids), disable=disable_tqdm):

        #     try:
        #         res: Tuple[Optional[Location], Optional[float]] = self._fetch_gauge_qpe(_id, start_time, end_time)
        #     except Exception as e:
        #         # HACK: silencing these errors for now
        #         # print(e)
        #         # print(f"Error fetching gauge id: {_id}")
        #         continue

        #     # remove invalid results
        #     # TODO: clarify this >>
        #     if res[0] == None or res[1] == None: continue
            
        #     all_gauge_qpe.append({
        #         "station_id": _id,
        #         "lat": res[0].lat,
        #         "lon": res[0].lon + 360,
        #         "qpe": res[1],
        #     })

        return all_gauge_qpe

    def _grid_all_gauge_qpe(self, gauge_qpes: List[Tuple[Location, float]]) -> List[List[float]]: 
        """
        Convert a list of (Location, precip: float) values into a 2D grid of precipitation values (in.)
        # TODO: how do we map MRMS data and our rain gauge data to the same 2D cartesian grid?
        """

        def _latlon_to_idx(lat, lon) -> Tuple[int, int]:
            """
            Return (i, j) indices on grid.
            """
            i = int((lat - self._LAT_MIN) / self._DLAT)
            j = int((lon - self._LON_MIN) / self._DLON)
            return i, j

        # 1-km grid covering the CCRFCD domain
        lat_bins = np.arange(self._LAT_MIN, self._LAT_MAX + self._DLAT, self._DLAT)
        lon_bins = np.arange(self._LON_MIN, self._LON_MAX + self._DLON, self._DLON)

        grid_sum = np.full((len(lat_bins), len(lon_bins)), 0.0)
        grid_cnt = np.zeros_like(grid_sum)

        for loc, precip in gauge_qpes:
            
            # skip bad gauges
            if precip is None:
                continue
            
            i, j = _latlon_to_idx(loc.lat, loc.lon)
            
            if 0 <= i < grid_sum.shape[0] and 0 <= j < grid_sum.shape[1]:
                grid_sum[i, j] += float(precip)
                grid_cnt[i, j] += 1

        with np.errstate(invalid="ignore"):
            grid_mean = grid_sum / grid_cnt

        return grid_mean

    def _fetch_ccrfcd_qpe_xhr(self, end_time: datetime, delta_hr: int = 0, delta_min: int = 0) -> List[Dict]:
        """
        - TODO: skip gridding; return raw lat/lon gauage's w/ ids.
        
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        start_time = end_time - timedelta(hours=delta_hr, minutes=delta_min)
        pts        = self._fetch_all_gauge_qpe(start_time, end_time)
        return pts

    def fetch_ccrfcd_qpe_1hr(self, end_time: datetime) -> List[Dict]: 
        """
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        return self._fetch_ccrfcd_qpe_xhr(end_time, delta_hr=1)

    def fetch_ccrfcd_qpe_3hr(self, end_time: datetime) -> np.ndarray: 
        """
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        return self._fetch_ccrfcd_qpe_xhr(end_time, delta_hr=3)

    def fetch_ccrfcd_qpe_6hr(self, end_time: datetime) -> np.ndarray: 
        """
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        return self._fetch_ccrfcd_qpe_xhr(end_time, delta_hr=6)

    def fetch_ccrfcd_qpe_12hr(self, end_time: datetime) -> np.ndarray: 
        """
        **Time Zone: PDT**
        - Fetch precip accumulation from ``end_time - 12:00`` to ``end_time``

        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inches)
        """
        return self._fetch_ccrfcd_qpe_xhr(end_time, delta_hr=12)

    def fetch_ccrfcd_qpe_24hr(self, end_time: datetime) -> np.ndarray: 
        """
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        return self._fetch_ccrfcd_qpe_xhr(end_time, delta_hr=24)

    def fetch_ccrfcd_qpe_48hr(self, end_time: datetime) -> np.ndarray: 
        """
        Returns
        ---
        - An [N, M] array containing cumlative precipitation values (inch)
        """
        return self._fetch_ccrfcd_qpe_xhr(end_time, delta_hr=48)

if __name__ == "__main__": 

    t1 = datetime(year=2024, month=7, day=14, hour=6)
    t2 = datetime(year=2024, month=7, day=14, hour=12)
    obj = CCRFCDClient()
    gauge_qpes = obj.fetch_ccrfcd_qpe_1hr(t1)
