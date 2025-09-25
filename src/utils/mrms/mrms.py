"""
# Sources
---
- https://github.com/NOAA-National-Severe-Storms-Laboratory/mrms-support
- https://training.weather.gov/wdtd/courses/MRMS/lessons/overview-v12.2/presentation_html5.html
- https://github.com/HumphreysCarter/mrms-api

# Dataset Structure
---
- DOMAIN
    - PRODUCT
        - YYYYMMDD
            - PRODUCT_YYYYMMDD-ZZZZZZ.grib2.gz
"""
 
import re
import xarray
import subprocess

from enum import Enum
from pathlib import Path
from datetime import datetime
from s3fs import S3FileSystem
from typing import List, Optional
from urllib.parse import urljoin, urlparse


class MRMSDomain:

    CONUS = "CONUS"


class MRMSURLs:

    BASE_URL = "s3://noaa-mrms-pds"
    BASE_URL_CONUS = "s3://noaa-mrms-pds/CONUS"


class MRMSFileName:

    def __init__(self, name_str: str):
        """
        Expecting `name_str` format:
        - `MRMS_{PRODUCT_NAME}_{yyyymmdd}-{hhmmss}.grib2.gz`
        """
        
        pattern = r"^MRMS_(.+)_(\d{8})-(\d{6})\.grib2\.gz$"
        match = re.match(pattern, name_str)
        
        if not match:
            raise ValueError(f"Filename '{name_str}' does not match the expected format.")

        self.product = match.group(1)
        
        date_str = match.group(2)
        time_str = match.group(3)
        
        self.datetime = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
        self._str = name_str

    def __str__(self) -> str:
        return self._str


class MRMSPath:

    def __init__(self, 
                 domain: Optional[str] = None,
                 product: Optional[str] = None,
                 yyyymmdd: Optional[str] = None,
                 file_name: Optional[str] = None):
        
        self.domain = domain
        self.product = product
        self.yyyymmdd = yyyymmdd
        self.file_name = file_name
        self.path = self._build_path()

    def _build_path(self) -> str:
        segments = [MRMSURLs.BASE_URL]

        if self.domain:
            segments.append(self.domain)
        elif any([self.product, self.yyyymmdd, self.file_name]):
            raise ValueError("'domain' must be specified if subsequent fields are set.")

        if self.product:
            segments.append(self.product)
        elif any([self.yyyymmdd, self.file_name]):
            raise ValueError("'product' must be specified if subsequent fields are set.")

        if self.yyyymmdd:
            segments.append(self.yyyymmdd)
        elif self.file_name:
            raise ValueError("'yyyymmdd' must be specified if 'file_name' is set.")

        if self.file_name:
            segments.append(self.file_name)

        # Ensure correct joining of URL segments with '/'
        return '/'.join(segments)

    def get_basename(self) -> Optional[str]:
        return self.file_name

    def get_base_datetime(self) -> Optional[datetime]:
        """
        Expecting ``self.file_name`` format:
        - ``MRMS_{PRODUCT_NAME}_{yyyymmdd}-{hhmmss}.grib2.gz``
        """
        
        if self.file_name == None: return None
        mrms_file_name = MRMSFileName(self.file_name)
        return mrms_file_name.datetime

    def __str__(self) -> str:
        return self.path

    @classmethod
    def from_str(cls, path_str: str) -> 'MRMSPath':
        
        parsed_url = urlparse(path_str)
        parts = [part for part in parsed_url.path.split('/') if part]

        try:
            data_idx = parts.index('noaa-mrms-pds')
            relevant_parts = parts[data_idx + 1:]
        except ValueError:
            raise ValueError("'data' segment not found in URL path.")

        domain = product = yyyymmdd = file_name = None

        if len(relevant_parts) >= 1:
            domain = relevant_parts[0]
        if len(relevant_parts) >= 2:
            product = relevant_parts[1]
        if len(relevant_parts) >= 3:
            yyyymmdd = relevant_parts[2]
        if len(relevant_parts) >= 4:
            file_name = relevant_parts[3]

        return cls(domain=domain, product=product, yyyymmdd=yyyymmdd, file_name=file_name)


class MRMSProducts:
    """
    An enumeration of all available MRMS CONUS products at a given moment.
    """

    def __init__(self):
        self.products = MRMSProducts._fetch_products()

    @staticmethod
    def _fetch_products() -> List[str]:
        s3_file_system = S3FileSystem(anon=True)
        results = s3_file_system.ls(MRMSURLs.BASE_URL_CONUS)
        products = []
        for res in results:
            products.append(res.split('/')[-1])
        return products


class MRMSAWSS3Client:
    """
    A high-level python API for the public MRMS AWS S3 bucket.
    """

    def __init__(self, format="NCEP"):

        # create an anonymous fs
        self.s3_file_system = S3FileSystem(anon=True)
        self.format = format

    def ls(self, path: str) -> List[str]:
        return self.s3_file_system.ls(path)

    def download(self, path: str, to: str, recursive=False) -> List[str] | str:
        """
        Returns
        ---
        - A list of `str` paths corresponding to successfully downloaded files.
        """

        assert self.s3_file_system.exists(path), f"Error! Invalid path: {path}"
        # assert Path(to).is_dir(), f"Error! 'To' not a valid dir: {to}"

        # if  : recursive is true than path msut always be a dir
        # else: path must be a file + file name must be appended to end of "to"
        remote_files = [path]
        if recursive == True:
            assert path.endswith("/"), (
                "When recursive=True the S3 path must end with '/' so it is "
                "interpreted as a prefix, not a single object."
            )
            remote_entries = self.s3_file_system.ls(path, detail=True)
            remote_files = [e["Key"] for e in remote_entries if e["type"] == "file"]
        else:
            assert not path.endswith("/"), (
                "When recursive=False the S3 path must point to a single object, "
                "not a directory prefix."
            )

        # map remote keys to download dst paths
        dst_root = Path(to).expanduser().resolve()
        local_paths: List[str] = []
        if recursive:
            prefix_len = len(path)
            for key in remote_files:
                rel_key = key[prefix_len:]
                local_paths.append(str(dst_root / rel_key))
        else:
            local_paths.append(str(dst_root / Path(path).name))

        # try to download files -> "to"
        cmd = ["aws", "s3", "cp", path, str(to), "--no-sign-request"]
        if recursive:
            cmd.append("--recursive")

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            from pprint import pprint
            pprint(f"Command failed to execute: {cmd}")
            raise RuntimeError(
                f"Download failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
            )

        # TODO: clarify; wtf is this
        if len(local_paths) == 1:
            return local_paths[0]
        
        return local_paths

    def submit_bulk_download(self, paths: List[str], tos: List[str]): ...


if __name__ == "__main__":
    prod = MRMSProducts()
    client = MRMSAWSS3Client()
    res = client.ls(MRMSURLs.BASE_URL_CONUS)
    breakpoint()
