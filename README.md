# LSH-Based Recommender System for MovieLens

## Overview

This project implements an item-item collaborative filtering recommender system using Locality-Sensitive Hashing (LSH) to efficiently compute item similarities at scale. The goal is to evaluate performance across different MovieLens dataset sizes, progressing from a clear prototype to a tuned and production-ready implementation.

## Project Structure

1. `prototype.ipynb` $\\ $ Initial implementation with clear explanations. Focused on conceptual validation using a small dataset.

2. `tuning.ipynb` $\\ $ Hyperparameter optimization and performance tuning.

    - `tuning.json` $\\ $ Stores results and evaluation metrics for each tested configuration.

3. `deploy.py` $\\ $ Optimized version for efficient execution. No debug blocks or intermediate visualizations. Suitable for Spark standalone or distributed environments.

    - `results.csv` $\\ $ Final evaluation metrics for each dataset: RMSE, MAE, elapsed time, etc.

    - `results/` $\\ $ Directory containing generated prediction files for each dataset.

**Note:** Code may vary slightly between stages (prototype → tuning → deploy), but the core logic remains the same.

## MovieLens Datasets

### MovieLens 100k

- [Download](https://files.grouplens.org/datasets/movielens/ml-latest-small.zip) (1 MB)

- Small: 100,000 ratings and 3,600 tag applications applied to 9,000 movies by 600 users. Last updated 9/2018.

```bash
spark-submit deploy.py data/100k.csv
```       

### MovieLens 1M

- [Download](https://files.grouplens.org/datasets/movielens/ml-1m.zip) (6 MB)

- MovieLens 1M movie ratings. Stable benchmark dataset. 1 million ratings from 6000 users on 4000 movies. Released 2/2003.

```bash
spark-submit deploy.py data/1M.csv
```

### MovieLens 10M

- [Download](https://files.grouplens.org/datasets/movielens/ml-10m.zip) (63 MB)

- MovieLens 10M movie ratings. Stable benchmark dataset. 10 million ratings and 100,000 tag applications applied to 10,000 movies by 72,000 users. Released 1/2009.

```bash
```