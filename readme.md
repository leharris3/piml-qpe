# **PI-ML QPE for the western CONUS**
---
> *Work in-progress.*

[![Hugging Face Dataset](https://img.shields.io/badge/Hugging%20Face-Dataset-FFD21E?logo=huggingface&logoColor)](https://huggingface.co/datasets/leharris3/ccrfcd-mrms-hrrr-env-2021-2025)

*This is the official repository for an ML Quantitative Precipitation Estimation (ML-QPE) algorithm and accompanying dataset developed in collaboration with the National Weather Service, the University of Oklahoma, the University of Iowa, and the University of North Carolina Chapel Hill.*

The following `readme` introduces the major components of this codebase, including:

1. 2021-2025 MRMS QPE dataset
    - 1M+ samples
    - 220 rain gauge sites
    - HRRR native + derived environmental fields
    - All data aligned to **1km/2min** resolution
2. Data analysis/plotting scripts
3. ML-QPE algorithm + benchmarks*

\*In development

***

## Dataset quickstart

Download from **Hugging Face**.

```bash
pip install datasets
```

```python
from datasets import load_dataset

# download the dataset; convert to a dataframe
dataset = load_dataset("leharris3/ccrfcd-mrms-hrrr-env-2021-2025")
df = dataset['train'].to_pandas()
```

![/assets/hrrr-analysis](/assets/hrrr-analysis.gif)

### Acknowledgements

A big thanks to [**UNITES**](https://tianlong-chen.github.io/index.html#lab) and [**L$^3$**](https://www.ssriva.com) at UNC Chapel Hill for lending computing resources to this project.
