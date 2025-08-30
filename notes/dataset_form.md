# Dataset QC
---

## TODO

### Alignment

- [x] 1. compare PDT/PST events; radar loops
    - [x] 1a. make a gif/radar loop for *any* event
    - [x] 1b. rewrite the mrms, data-fetching backend
    - [x] 1c. improve gauge visualization (i.e., no disappearing gauges, better rg icons) ]
    - [x] 1d. add a "top gauges" panel, showing the current top-4 rain gauges and their vals
- [ ] 2. 

---

- Timescales
    - $t \in \mathcal{T}_{MRMS}$: timestep in MRMS dataset
        - $t \in \{0, ..., n \}$
    - $u \in \mathcal{T}_{CCRFCD}$: timestep in rain gauge dataset
        - $u \in \{0, ..., m \}$
    > *A fundamnetal problem is that the MRMS and CCRFCD datasets are on different timescales, meaning temporal alignment is required*

- MRMS dataset
    - dims: lat, lon, time
    - $MRMS_{t} \in \real^3$
    - $MRMS_{t} = \{mrms_{0, 0}, ..., mrms_{i, j}\}$

- CCRFCD dataset
    - dims: gauge_idx, time
    - $k \in \{0, ..., o\}$: set of *unique* rain gauge indices
    - $\mathcal{I}_k = (lat, lon)$: rain guage locations
    - $\mathcal{G}_u = \{g_0, ..., g_p \}$: dataset of historical rain accumulation

- Aligned dataset
    - dims: gauge_idx, source (MRMS/CCRFCD), time
    - We want to generate a dataset $\mathcal{D} \in \real^3$ where for a timestep $t$ and gauge index $k$...
        - $D_{t, k} = ( MRMS_{t, \mathcal{I}_k}, \mathcal{G}_{t, k} )$
        - *But what if $k \notin \mathcal{T}_{MRMS}$?*

##### Generating an aligned dataset

1. Aggregate rain gauge raw data (i.e., bucket tips) into a ***sparse*** table with timestep index $\mathcal{T}_{CCRFCD}$
    - Why is this table "sparse"?
        - Timesteps $u \in \mathcal{T}_{CCRFCD}$ are not uniformly spaced
        - Each timestep $u$ (in theory) corresponds to **one** datapoint for **one** rain gauge
    - Sort `datetime-local` column 

| gauge-idx | datetime-local | utc-offset | datetime-utc | val |
| :---: | :---: | :---: | :---: | :---: |
| 4 | 2025-03-03-07:03 | -7 | 2025-03-03-14:03 | 3.5 |
| 4 | 2025-03-03-07:35 | -7 | 2025-03-03-14:35 | 3.6 |

2. Generate a ***dense*** table from `1` with a timestep index of $\mathcal{T}_{MRMS}$
    - > $\forall_{t \in \mathcal{T}_{MRMS}} \forall_{k \in \mathcal{I}} \exists $ *row in datatable*
    - 2a. $\forall_{t \in \mathcal{T}_{MRMS}} \forall_{k \in \mathcal{I}}$ ...
        - Lookup rows for $k$ between $[t_{start}, t_{end}]$

| gauge-idx | start-datetime-utc | end-datetime-utc | val |
| :---: | :---: | :---: | :---: |
| 4 | 2025-03-03-14:00 | 2025-03-03-15:00 | 3.5 |
| 4 | 2025-03-03-14:02 | 2025-03-03-15:02 | 3.5 |
| 4 | ... | 3.5 |
| 4 | 2025-03-03-14:30 | 2025-03-03-15:30 | 3.6 |
| 5 | 2025-03-03-14:30 | 2025-03-03-15:30 | `NaN` |

3. select $\mathcal{T}_{MRMS}$; i.e., determine which MRMS 1H-QPE timesteps to download
    - previous hueristic:
        - 1 1kmx1km grid-cell in the LV valley exceeded .25 1H QPE at some point in a 24H period
            - TODO: confirm
    - a smarter approach may be: download MRMS data based on CCRFCD data-coverage
        - generate table in `1.`
        - for every hour in 2021-2025 (step=2 minutes)...

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