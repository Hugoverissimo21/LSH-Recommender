# TITULOODISODISOADOAIS

## Project Structure

1. prototype.ipynb : criar um q funcione e bem explicado

2. tuning.ipynb : melhorar os hyperparâmetros e pequenos tweaks no codigo

    - tunning.json: resultados do tuning

3. deploy.py : mais eficiente, e menos debugging-friendly

    - results.csv : resultados do deploy.py

    - results/ : pasta com predictions para cada dataset

## MovieLens Datasets

### 100k

100k = https://files.grouplens.org/datasets/movielens/ml-latest-small.zip (size: 1 MB)

- Small: 100,000 ratings and 3,600 tag applications applied to 9,000 movies by 600 users. Last updated 9/2018.

```bash
spark-submit deploy.py data/100k.csv
```       

### 1M

1M = https://files.grouplens.org/datasets/movielens/ml-1m.zip (size: 6 MB)

- MovieLens 1M movie ratings. Stable benchmark dataset. 1 million ratings from 6000 users on 4000 movies. Released 2/2003.

```bash
spark-submit deploy.py data/1M.csv
```

### 10M

10M = https://files.grouplens.org/datasets/movielens/ml-10m.zip (size: 63 MB)

- MovieLens 10M movie ratings. Stable benchmark dataset. 10 million ratings and 100,000 tag applications applied to 10,000 movies by 72,000 users. Released 1/2009.

```bash
```