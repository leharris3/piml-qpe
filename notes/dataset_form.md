# **Dataset creation**
---

### Definitions

- Timescales
    - $t \in \mathcal{T}_{MRMS}$: timestep in MRMS dataset
        - $t \in \{0, ..., n \}$
    - $u \in \mathcal{T}_{CCRFCD}$: timestep in rain gauge dataset
        - $u \in \{0, ..., m \}$
    > *A fundamnetal problem is that the MRMS and CCRFCD datasets are on different timescales, meaning temporal alignment is required*

- MRMS dataset
    - dims: lattitude $(H)$, lon $(W)$, time
    - $MRMS_{t} \in \mathbb{R}^{H \times W}$
    - $MRMS_{t} = \{mrms_{0, 0}, ..., mrms_{i, j}\}$

- CCRFCD dataset
    - dims: gauge_idx, time
    - $\mathcal{I}_k = (lat, lon)$: rain guage location
        - $k \in \{0, ..., o\}$: element in set of *unique* rain gauge indices
    - $\mathcal{G}_u = \{g_0, ..., g_p \}$: dataset of historical rain accumulation

- Aligned dataset
    - dims: gauge_idx, source (MRMS/CCRFCD), time
    - We want to generate a dataset $\mathcal{D} \in \mathbb{R}^{I \times 2 \times \mathcal{T}_{MRMS}}$ where for a timestep $t$ and gauge index $k$...
        - $D_{t, k} = ( MRMS_{t, \mathcal{I}_k}, \mathcal{G}_{t, k} )$

### Generating an aligned dataset

1. Aggregate rain gauge raw data (i.e., bucket tips) into a ***sparse*** table with timestep index $\mathcal{T}_{CCRFCD}$
    - Why is this table "sparse"?
        - Timesteps $u \in \mathcal{T}_{CCRFCD}$ are not uniformly spaced
        - Each timestep $u$ (in theory) corresponds to **one** datapoint for **one** rain gauge
    - Sort `datetime-local` column 

| gauge-idx | datetime-local | utc-offset | datetime-utc | val |
| :---: | :---: | :---: | :---: | :---: |
| 4 | 2025-03-03-07:03 | -7 | 2025-03-03-14:03 | 3.5 |
| 4 | 2025-03-03-07:35 | -7 | 2025-03-03-14:35 | 3.6 |

2. select $\mathcal{T}_{MRMS}$; i.e., determine which MRMS 1H-QPE timesteps to download
    - LV valley: (35.8, 36.4 / -115.4, -114.8)
    - 1 1km $\times$ 1km grid-cell in the LV valley >= 0.25 in. 1H QPE in a 24H period (i.e., 00:00-23:59 UTC)

3. Generate a ***dense*** table from `1` with a timestep index of $\mathcal{T}_{MRMS}$
- > $\forall_{t \in \mathcal{T}_{MRMS}} \forall_{k \in \mathcal{I}} \exists $ *row in datatable*

- 3a. $\forall_{t \in \mathcal{T}_{MRMS}} \forall_{k \in \mathcal{I}}$ ...
    - Lookup rows for $k$ between $[t_{start}, t_{end}]$
    - Calculate the *sum* of **positive** differences only
        - e.g., `[1, 1.2, 0.0, 0.3] -> [NaN, 0.2, NaN, 0.3] -> 0.5`
        - Rain gauges occasionally *reset*; negative rainfall amounts are impossible

| gauge-idx | start-datetime-utc | end-datetime-utc | gauge-1h-acc |
| :---: | :---: | :---: | :---: |
| 4 | 2025-03-03-14:00 | 2025-03-03-15:00 | 0.1 |
| 4 | 2025-03-03-14:02 | 2025-03-03-15:02 | 0.2 |
| 4 | ... | ... | ... |
| 4 | 2025-03-03-14:30 | 2025-03-03-15:30 | 0.1 |
| 5 | 2025-03-03-14:30 | 2025-03-03-15:30 | `NaN` |

4. Append MRMS 1H QPE; $\forall_{t \in \mathcal{T}_{MRMS}} \forall_{k \in \mathcal{I}}$
    - 4a. Get gauge location: $\mathcal{I}_k = (lat, lon)$
    - 4b. Find nearest MRMS grid-cell to rain gauge $k' = (lat', lon') \approx (lat, lon)$
    - 4c. Get $MRMS_{t, k'}$; append to aligned dataset $D$
        - $D_{t, k, mrms} = $ $ MRMS_{t, k'} \over{25.4} $
        - > *Note: we divide by 25.4 to convert MRMS data from millimeters to inches*

| gauge-idx | start-datetime-utc | end-datetime-utc | gauge-1h-acc | mrms-1h-qpe |
| :---: | :---: | :---: | :---: | :---: |
| 4 | 2025-03-03-14:00 | 2025-03-03-15:00 | 0.1 | 0.13 |
| 4 | 2025-03-03-14:02 | 2025-03-03-15:02 | 0.2 | 0.19 |
| 4 | ... | ... | ... | ... |
| 4 | 2025-03-03-14:30 | 2025-03-03-15:30 | 0.1 | 0.25 |
| 5 | 2025-03-03-14:30 | 2025-03-03-15:30 | `NaN` | 0.08 |

- > *Note: we should have `mrms-1h-acc` values for all rows in this dataset, but not necessarily `gauge-1h-acc` values*

##### Daylight Saving's Time

| year | month | day | local-time | utc |
| :---: | :---: | :---: | :---: | :---: | 
| 2021 | 3 | 14 | 2:00 AM | UTC−8 -> UTC−7 |
| 2021 | 11 | 7 | 2:00 AM | UTC−7 -> UTC−8 |
| 2022 | 3 | 13 | 2:00 AM | UTC−8 -> UTC−7 |
| 2022 | 11 | 6 | 2:00 AM | UTC−7 -> UTC−8 |
| 2023 | 3 | 12 | 2:00 AM | UTC−8 -> UTC−7 |
| 2023 | 11 | 5 | 2:00 AM | UTC−7 -> UTC−8 |
| 2024 | 3 | 10 | 2:00 AM | UTC−8 -> UTC−7 |
| 2024 | 11 | 3 | 2:00 AM | UTC−7 -> UTC−8 |
| 2025 | 3 | 9 | 2:00 AM | UTC−8 -> UTC−7 |