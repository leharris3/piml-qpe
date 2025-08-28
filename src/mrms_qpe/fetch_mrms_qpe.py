import os
import warnings
import xarray as xr

from glob import glob
from typing import List
from datetime import datetime, timedelta
from bisect import bisect_left, bisect_right
from concurrent.futures import ProcessPoolExecutor, as_completed

from src.utils.mrms.files import ZippedGrib2File, Grib2File
from src.utils.mrms.mrms import MRMSDomain, MRMSPath
from src.utils.mrms.mrms import MRMSAWSS3Client
from src.utils.mrms.products import MRMSProductsEnum


warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    message=".*decode_timedelta will default to False.*"
)

warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message=".*pyproj unable to set PROJ database path.*"
)


def _process_single_file(fp: str, to_dir: str) -> xr.Dataset:
            
    # HACK:...
    _fp = glob(f"{to_dir}/*{os.path.basename(fp)}")[0]
    zipped_gf = ZippedGrib2File(_fp)
    gf        = zipped_gf.unzip(to_dir=to_dir)
    xa        = gf.to_xarray()
    return xa


class MRMSQPEClient:
    """
    Wrapper for the MRMS AWS bucket; specifically for fetching 1H Radar-Only QPE.
    """

    def __init__(self):
        self.mrms_client = MRMSAWSS3Client()

    def _get_closest_file(self, paths: List[str], start_time: datetime, mode="nearest") -> str:
        
        if not paths:
            raise ValueError("Received an empty list of paths.")

        # 1. Build and sort (datetime, path) pairs
        dt_path_pairs = sorted(
            ((MRMSPath.from_str(fp).get_base_datetime(), fp) for fp in paths),
            key=lambda t: t[0],
        )
        dts = [p[0] for p in dt_path_pairs]  # just the datetimes
        mode = (mode or "nearest").lower()

        # 2. Choose according to mode
        if mode == "nearest":
            chosen_dt, chosen_fp = min(
                dt_path_pairs, key=lambda t: abs(t[0] - start_time)
            )
            return chosen_fp

        elif mode == "first":
            idx = bisect_right(dts, start_time) - 1
            if idx < 0:
                raise ValueError(
                    "No file time is ≤ start_time; cannot satisfy mode='first'."
                )
            return dt_path_pairs[idx][1]

        elif mode == "next":
            idx = bisect_left(dts, start_time)
            if idx >= len(dts):
                raise ValueError(
                    "No file time is ≥ start_time; cannot satisfy mode='next'."
                )
            return dt_path_pairs[idx][1]

        else:
            raise ValueError(f"Unrecognized mode '{mode}'. "
                             "Choose 'nearest', 'first', or 'next'.")

    def _fetch_radar_only_qpe_x(
            self, 
            end_time: datetime, 
            product: str, 
            mode="nearest", 
            time_zone="UTC", 
            to_dir="__temp",
            del_tmp_files=False,
        ) -> xr.Dataset | None:
        """
        **Timezone**: ``UTC``
        Fetch MRMS ``RadarOnly_QPE`` suite of products. 

        Args
        ---
        :start_time: ``end_time``
        :mode: ``str`` 
        - "nearest", "first", or "next"
            - "nearest": find the closest valid file to provide ``datetime``
            - "first"  : closest valid file whos time < start_time
            - "next"   : closest valid file whos time > start_time

        Returns
        ---
        """

        # TODO: add support for many, many timezones by using an existing library
        # HACK: PDT -> UTC
        if time_zone == "PDT":
            end_time += timedelta(hours=7)

        yyyymmdd = end_time.strftime("%Y%m%d")
        basepath = MRMSPath(
            domain   = MRMSDomain.CONUS, 
            product  = product,
            yyyymmdd = yyyymmdd
            )
        
        try:
            file_paths   = self.mrms_client.ls(str(basepath))
        except:
            print(f"Error: no MRMS file @{str(basepath)}")
            return None

        nearest_path = self._get_closest_file(file_paths, end_time)
        
        # TODO: del grib2 files after download
        mp = MRMSPath.from_str(nearest_path)

        # current pipeline: download -> unzip -> convert to xarray -> cleanup
        fp = self.mrms_client.download(str(mp), to=to_dir)
        zipped_gf = ZippedGrib2File(fp)
        gf        = zipped_gf.unzip(to_dir=to_dir)
        xa        = gf.to_xarray()

        if del_tmp_files == True:
            tmp_fps = glob(f"{to_dir}/**")
            for fp in tmp_fps:
                os.remove(fp)

        return xa
    
    def _fetch_radar_only_qpe_x_batch(
            self, 
            end_time: datetime, 
            product: str, 
            mode="nearest", 
            time_zone="UTC", 
            to_dir="__temp",
            del_tmp_files=False,
        ) -> List[xr.Dataset | None]:
        """
        **Timezone**: ``UTC``
        Fetch MRMS ``RadarOnly_QPE`` suite of products. 

        Args
        ---
        :start_time: ``end_time``
        :mode: ``str`` 
        - "nearest", "first", or "next"
            - "nearest": find the closest valid file to provide ``datetime``
            - "first"  : closest valid file whos time < start_time
            - "next"   : closest valid file whos time > start_time

        Returns
        ---
        """

        # HACK: PDT -> UTC
        if time_zone == "PDT":
            end_time += timedelta(hours=7)

        yyyymmdd = end_time.strftime("%Y%m%d")
        basepath = MRMSPath(
            domain   = MRMSDomain.CONUS, 
            product  = product,
            yyyymmdd = yyyymmdd
            )
        
        try:
            file_paths   = self.mrms_client.ls(str(basepath))
        except:
            print(f"Error: no MRMS file @{str(basepath)}")
            return None

        basepath_str = str(basepath)
        if not basepath_str.endswith("/"):
            basepath_str += "/"

        fps = self.mrms_client.download(basepath_str, to=to_dir, recursive=True)
        
        xas = []
        with ProcessPoolExecutor() as executor:
            futures = {executor.submit(_process_single_file, fp, to_dir): fp for fp in fps}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    xas.append(result)

        if del_tmp_files == True:
            tmp_fps = glob(f"{to_dir}/**")
            for fp in tmp_fps:
                os.remove(fp)

        return xas

    def fetch_radar_only_qpe_15m(self, end_time: datetime, mode="nearest", time_zone="UTC"):
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-0:15``-``end_time``
        """
        return self._fetch_radar_only_qpe_x(end_time, MRMSProductsEnum.RadarOnly_QPE_15M, mode=mode, time_zone=time_zone)
    
    def fetch_radar_only_qpe_1hr(self, end_time: datetime, mode="nearest", time_zone="UTC") -> xr.Dataset | None:
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-1:00``-``end_time``

        Params
        ---
        - :end_time:
        - :mode: {"nearest", "first"} 
            - When an MRMS product is unavailable for specified `end_time`, how we select the next closest item.
        - :time_zone: {"UTC", "PDT", "PST"}; default: "UTC"
        """
        return self._fetch_radar_only_qpe_x(end_time, MRMSProductsEnum.RadarOnly_QPE_01H, mode=mode, time_zone=time_zone)

    def fetch_radar_only_qpe_3hr(self, end_time: datetime, mode="nearest", time_zone="UTC"):
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-3:00``-``end_time``
        """
        return self._fetch_radar_only_qpe_x(end_time, MRMSProductsEnum.RadarOnly_QPE_03H, mode=mode, time_zone=time_zone)
    
    def fetch_radar_only_qpe_6hr(self, end_time: datetime, mode="nearest", time_zone="UTC"):
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-6:00``-``end_time``
        """
        return self._fetch_radar_only_qpe_x(end_time, MRMSProductsEnum.RadarOnly_QPE_06H, mode=mode, time_zone=time_zone)
    
    def fetch_radar_only_qpe_12hr(self, end_time: datetime, mode="nearest", time_zone="UTC"):
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-12:00``-``end_time``
        """
        return self._fetch_radar_only_qpe_x(end_time, MRMSProductsEnum.RadarOnly_QPE_12H, mode=mode, time_zone=time_zone)
    
    def fetch_radar_only_qpe_24hr(self, end_time: datetime, mode="nearest", time_zone="UTC"):
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-24:00``-``end_time``
        """
        return self._fetch_radar_only_qpe_x(end_time, MRMSProductsEnum.RadarOnly_QPE_24H, mode=mode, time_zone=time_zone)
    
    def fetch_radar_only_qpe_full_day_1hr(self, end_time: datetime, mode="nearest", time_zone="UTC", del_tmps=False) -> List[xr.Dataset]:
        """
        **Time Zone**: ``UTC``
        - Fetch ``end_time-24:00``-``end_time``
        """
        return self._fetch_radar_only_qpe_x_batch(end_time, MRMSProductsEnum.RadarOnly_QPE_01H, mode=mode, time_zone=time_zone, del_tmp_files=del_tmps)


if __name__ == "__main__":
    client = MRMSQPEClient()
    date = datetime.now()
    ar = client.fetch_radar_only_qpe_full_day_1hr(date, del_tmps=True)
    breakpoint()
