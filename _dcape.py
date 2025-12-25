"""
Minimal DCAPE calculation using MetPy.
"""

import pandas as pd

df = pd.read_csv("data/2021-01-01_2025-07-25_hrrr_env_final_v4.csv")
df.columns

import numpy as np
from metpy.units import units
from metpy.calc import downdraft_cape

# Pre-create unit objects to avoid repeated creation
_hPa = units.hPa
_K = units.K


def calc_dcape(
    surface_pres_pa,
    temp_2m_k, dewpoint_2m_k,
    temp_850_k, dewpoint_850_k,
    temp_700_k, dewpoint_700_k,
    temp_500_k, dewpoint_500_k
):
    """
    Calculate DCAPE from sparse pressure level data.
    Returns:
        DCAPE in J/kg, or np.nan if calculation fails
    """
    try:
        sfc_p_hpa = surface_pres_pa * 0.01  # Multiply is faster than divide
        
        # Build profiles - pre-sized lists avoid repeated growth
        p_list = [sfc_p_hpa]
        t_list = [temp_2m_k]
        td_list = [dewpoint_2m_k]
        
        # Unrolled loop - avoids tuple unpacking overhead
        if 850 < sfc_p_hpa:
            p_list.append(850)
            t_list.append(temp_850_k)
            td_list.append(dewpoint_850_k)
        
        if 700 < sfc_p_hpa:
            p_list.append(700)
            t_list.append(temp_700_k)
            td_list.append(dewpoint_700_k)
        
        if 500 < sfc_p_hpa:
            p_list.append(500)
            t_list.append(temp_500_k)
            td_list.append(dewpoint_500_k)
        
        if len(p_list) < 2:
            return np.nan
        
        # Use pre-created unit objects
        p = np.array(p_list) * _hPa
        T = np.array(t_list) * _K
        Td = np.array(td_list) * _K
        
        dcape, _, _ = downdraft_cape(p, T, Td)
        return dcape.magnitude
        
    except Exception:
        return np.nan  # Removed slow print statement


def proc_row_dcape(row):
    return calc_dcape(
        row["surface_PRES"],
        row["2m_above_ground_TMP"],
        row["2m_above_ground_DPT"],
        row["850mb_TMP"],
        row["850mb_DPT"],
        row["700mb_TMP"],
        row["700mb_DPT"],
        row["500mb_TMP"],
        row["500mb_DPT"],
    )


# Move tqdm.pandas() call to execution time
from tqdm import tqdm
tqdm.pandas()
dcape = df.progress_apply(proc_row_dcape, axis=1)

breakpoint()