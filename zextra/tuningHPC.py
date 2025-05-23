# %% [markdown]
# Use CV Validation to get the best hyperparameters for the LSH model:
# 
# - `bucketLength` $\in [0.5, 2.0]$ based on some small research 
# 
# - `numHashTables` $\in [1, 10]$ so the number of hash tables is not too large
# 
# - `approxSimilarityJoin` threshold $\in [0, 1.41]$ so the cosine angle can be at most $90 \degree$

# %% [markdown]
# ---

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct
from pyspark.ml.linalg import Vectors, SparseVector
from pyspark.sql import Row, DataFrame
from pyspark.ml.feature import Normalizer
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as sql_sum, col
from pyspark.sql.functions import coalesce, lit
from pyspark.ml.evaluation import RegressionEvaluator

import os
import json
import numpy as np
from functools import reduce
import time
import matplotlib.pyplot as plt

# %% [markdown]
# # Treat the data

# %%
spark = SparkSession.builder \
    .appName("ItemItemCF") \
    .master("local[8]") \
    .config("spark.executor.cores", "8") \
    .config("spark.driver.memory", "50g") \
    .config("spark.executor.memory", "50g") \
    .config("spark.memory.fraction", "0.9") \
    .config("spark.shuffle.spill.compress", "true") \
    .config("spark.shuffle.compress", "true") \
    .getOrCreate()

data = spark.read.csv("data/1M.csv", header=True, inferSchema=True) \
            .select("userId", "movieId", "rating")

# %%
# get 10 folds of data
folds = data.randomSplit([0.1]*10, seed=42)

# %% [markdown]
# # Create functions automate the CV

# %%
def item_item_cf_similarities(ratings, bucketLength_in, numHashTables_in, threshold_in):
    # get the number of unique users
    num_users = ratings.select("userId").distinct().count()

    # create the sparse vector for movie function
    def to_sparse_vector(user_ratings, size):
        # Sort by userId to get strictly increasing indices
        sorted_pairs = sorted(user_ratings, key=lambda x: x.userId)
        indices = [x.userId - 1 for x in sorted_pairs]
        values = [x.rating for x in sorted_pairs]
        return Vectors.sparse(size, indices, values)

    # group by movieId and collect user ratings
    item_user = ratings.groupBy("movieId") \
        .agg(collect_list(struct("userId", "rating")).alias("user_ratings"))

    # convert that to a sparse vector
    item_vector_rdd = item_user.rdd.map(
        lambda row: Row(
            movieId=row["movieId"],
            features=to_sparse_vector(row["user_ratings"], num_users)
        )
    )

    # convert to DataFrame because of Normalizer (MLlib)
    item_vectors = spark.createDataFrame(item_vector_rdd)

    # normalizing with L2 (Euclidean) norm (p=2)
    normalizer = Normalizer(inputCol="features", outputCol="norm_features", p=2.0)
    normalized = normalizer.transform(item_vectors)

    # create the LSH model
    lsh = BucketedRandomProjectionLSH(
        inputCol="norm_features",
        outputCol="hashes",
        bucketLength=bucketLength_in,
        numHashTables=numHashTables_in
    )

    # fit the model
    lsh_model = lsh.fit(normalized)

    # get the approximate neighbors
    neighbors = lsh_model.approxSimilarityJoin(
        normalized,
        normalized,
        threshold=threshold_in,
        distCol="distance"
    ).filter(col("datasetA.movieId") < col("datasetB.movieId"))  # avoid bottom triangle (reverse + self)

    # convert the distance to cosine similarity
    neighbors_cosine = neighbors.withColumn(
        "cosine_sim",
        1 - (col("distance") ** 2) / 2
    ).select(
        col("datasetA.movieId").alias("movie_i"),
        col("datasetB.movieId").alias("movie_j"),
        "cosine_sim"
    )

    # add reverse pairs: (i,j) -> (i,j) and (j,i)
    reverse = neighbors_cosine.selectExpr("movie_j as movie_i", "movie_i as movie_j", "cosine_sim")
    similarities = neighbors_cosine.union(reverse)

    return similarities

# %%
def item_item_cf_predictions(ratings, similarities, test):
    # get the neighbors of the target movies
    test_with_ratings = test.alias("t") \
        .join(similarities.alias("s"), col("t.movieId") == col("s.movie_i")) \
        .join(ratings.alias("r"), (col("t.userId") == col("r.userId")) & (col("s.movie_j") == col("r.movieId"))) \
        .select(
            col("t.userId"),
            col("t.movieId").alias("target_movie"),
            col("s.movie_j").alias("neighbor_movie"),
            col("s.cosine_sim"),
            col("r.rating").alias("neighbor_rating")
        )

    # get the predicted rating
    predictions = test_with_ratings.groupBy("userId", "target_movie").agg(
        (sql_sum(col("cosine_sim") * col("neighbor_rating")) / sql_sum(col("cosine_sim"))).alias("pred_rating")
    )

    # join with the test set to get the actual rating
    final = predictions.alias("p").join(
        test.alias("t"),
        (col("p.userId") == col("t.userId")) & (col("p.target_movie") == col("t.movieId")),
        how="right"  # keep all test rows even if no prediction (no neighbors)
    ).select(
        col("t.userId"),
        col("t.movieId"),
        coalesce(col("p.pred_rating"), lit(3.0)).alias("pred_rating"),
        col("t.rating").alias("actual_rating")
    )

    return final

# %%
def item_item_cf_results(final):
    # RMSE
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="actual_rating", predictionCol="pred_rating")
    rmse = evaluator.evaluate(final)

    # MAE
    mae_evaluator = RegressionEvaluator(metricName="mae", labelCol="actual_rating", predictionCol="pred_rating")
    mae = mae_evaluator.evaluate(final)

    return rmse, mae

# %%
def item_item_cf_cv(train, test, hyperparameters):
    bucketLength, numHashTables, threshold = hyperparameters

    similarities = item_item_cf_similarities(train, bucketLength, numHashTables, threshold)
    predictions = item_item_cf_predictions(train, similarities, test)
    rmse, mae = item_item_cf_results(predictions)
    
    return rmse, mae

# %% [markdown]
# # Applying the CV

# %%
# open the tuningHPC.json file
if os.path.exists("tuningHPC.json"):
    with open("tuningHPC.json", 'r') as f:
        results = json.load(f)

else:
    results = {}

# %%
# get the hyperparameters search space
def sample_hyperparameters(n_samples):
    bucketLength = np.random.uniform(0.5, 2.0, size=n_samples).round(1)
    numHashTables = np.random.randint(1, 10, size=n_samples)
    threshold = np.random.uniform(0.1, 1.2, size=n_samples).round(1)

    return [(float(x), int(y), float(z)) for (x,y,z) in list(zip(bucketLength, numHashTables, threshold))]

# %%
# apply the hyperparameter tuning
n_samples = 50
hyperparameters = sample_hyperparameters(n_samples)

# using partial CV for quicker tuning
n_validations = 5

# cycle through the hyperparameters
for i, hyperparameter in enumerate(hyperparameters):
    cvalidation = True

    if False:
        print("Bypassing hyperparameter tuning")
        break

    bucketLength, numHashTables, threshold = hyperparameter
    print(f"Combination {i+1}/{n_samples}: bucketLength={bucketLength}, numHashTables={numHashTables}, threshold={threshold}")

    if str(hyperparameter) in results:
        continue

    avg_rmse, avg_mae, avg_time = 0, 0, 0
    # cycle through the folds
    for validation in range(n_validations):

        try:
            # get the training folds
            training_folds = [folds[i] for i in range(len(folds)) if i not in [validation, 9]]
            train = reduce(DataFrame.unionByName, training_folds)

            # get the validation fold
            val = folds[validation]

            # run the item-item CF
            start = time.time()
            rmse, mae = item_item_cf_cv(train, val, hyperparameter)
            end = time.time()

            avg_rmse += rmse
            avg_mae += mae
            avg_time += round(end - start, 2)

        except Exception as e:
            print(f"Error in combination {i+1}/{n_samples} for validation {validation}: {e}")
            cvalidation = False
            break

    # save the results
    if cvalidation:
        results[str(hyperparameter)] = {
            "bucketLength": bucketLength,
            "numHashTables": numHashTables,
            "threshold": threshold,
            "n_validations": n_validations,
            "rmse": avg_rmse / n_validations,
            "mae": avg_mae / n_validations,
            "time": avg_time / n_validations
        }
        with open("tuningHPC.json", 'w') as f:
            json.dump(results, f, indent=4)

# %%
with open("tuningHPC.json", 'w') as f:
    json.dump(results, f, indent=4)

# %% [markdown]
# # Choosing the best hyperparameters

# ...
