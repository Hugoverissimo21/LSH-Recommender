# [LSH-Based Recommender System for MovieLens](https://github.com/Hugoverissimo21/LSH-Recommender)

## Overview

This project implements an item-item collaborative filtering recommender system using Locality-Sensitive Hashing (LSH) to efficiently compute item similarities at scale. The goal is to evaluate performance across different MovieLens dataset sizes, progressing from a clear prototype to a tuned and production-ready implementation.

## Features

- Item-item collaborative filtering using cosine similarity.

- Locality-Sensitive Hashing (LSH) for scalable similarity search.

- Compatible with multiple MovieLens dataset sizes (100K, 1M, 10M+).

- PySpark-based for distributed computation.

- Modular design for tuning, evaluation, and export.

## Methodology and Results

comecei por fazer um prototpio localmente, com o 100k por ser o mais pequeno, tudo bem documentado no devido ficheiro (`prototype.ipynb`)

pelo facto de ter hyperapraemtros decidi fazer um partial cross validation, com o mesmo dataset, para ter uma ideia da melhor configuracao, e ao ter bons resultados com uma das configuracoes, tanto a nivel de rmse como de tempo, decidi avancar para o deploy para poder usar o sparl-submit

com a possiblidade de usar o HPC decidi fazer isso, contudo os resultados estavam mt a quem do esperado, comparando a execucao local com o hpc, os resultados do rmse eram smp mt piores, constantemente

inicialmente pensei que poderia ser devido a escolha dos hiperparementros pelas hash fuctions poderem variar entre os computadores, ent fiz um tunning no hpc, mas ao ver q os resultados continuavam a ser mt piores, ao investigar mais reparei que havia um problema q era a constante selecao do defaulta rating (3.0) por falta de vizinhos, apesar de existirem

isto acontecia por ter demasiadas particoes (>100) e ele falhar em encontrar os vizinhos em particoes diferentes, algo que nao acontecia localemnte por nao ser em tao larga escala (mas demorar mais tempo)

decidi entao tbm definir o numero de reparticoes e testar configuracoes diferentes, melhorando ligeiramente os resultados, diminuindo o desvio padrao, mas continuando a ser mt piores que os resultados locais

![Results: HPC, RMSE/size, Spark Repartitions](assets/results_rmse_spark_choice_hpc.png)
![Results: HPC, RMSE/size, Manual Repartitions](assets/results_rmse_small_manual_repartitioning_hpc.png)

falta correr no pc, ou concluir


.........................................

## Project Structure

1. `prototype.ipynb`<br>Initial implementation notebook with detailed explanations. Developed locally for conceptual validation using the smallest MovieLens dataset (`100k.csv`).

2. `tuning.ipynb`<br>Hyperparameter optimization and performance tuning notebook. Executed using `100k.csv` (local) and `1M.csv` (HPC) to refine similarity thresholds, number of hash functions and length of each hash bucket.

    - `tuningLOC.json`<br>Stores results and evaluation metrics for each tested configuration during local tuning runs.

    - `tuningHPC.json`<br>Stores results and evaluation metrics for each tested configuration during high-performance computing (HPC) runs.

3. `deploy.py`<br>Optimized version for efficient execution. No debug blocks or intermediate visualizations. Suitable for Spark standalone or distributed environments.

    - `deploy.sh`<br>HPC shell script to iterate over all dataset and execute `spark-submit` for each.

    - `deployHPC.csv`<br>Output file containing result metrics generated from the HPC run.

    - `deployHPC/`<br>Directory containing the predicted ratings generated from the HPC run.

    - `deploy vlocal.py`<br>........................................................

- `data/` and `assets/`<br>Contain auxiliary files.

**Note:** Code may vary slightly between stages (prototype -> tuning -> deploy), but the core logic remains the same.

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
spark-submit deploy.py data/10M.csv
```

### MovieLens 20M

- [Download](https://files.grouplens.org/datasets/movielens/ml-20m.zip) (190 MB)

- MovieLens 20M movie ratings. Stable benchmark dataset. 20 million ratings and 465,000 tag applications applied to 27,000 movies by 138,000 users. Includes tag genome data with 12 million relevance scores across 1,100 tags. Released 4/2015; updated 10/2016 to update links.csv and add tag genome data.

```bash
spark-submit deploy.py data/20M.csv
```

### MovieLens 25M

- [Download](https://files.grouplens.org/datasets/movielens/ml-25m.zip) (250 MB)

- MovieLens 25M movie ratings. Stable benchmark dataset. 25 million ratings and one million tag applications applied to 62,000 movies by 162,000 users. Includes tag genome data with 15 million relevance scores across 1,129 tags. Released 12/2019.

```bash
spark-submit deploy.py data/25M.csv
```