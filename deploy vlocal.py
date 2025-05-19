import argparse
import time
import os
import pandas as pd
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import collect_list, struct
from pyspark.ml.linalg import Vectors, SparseVector
from pyspark.sql import Row, DataFrame
from pyspark.ml.feature import Normalizer
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as sql_sum, col
from pyspark.sql.functions import coalesce, lit
from pyspark import StorageLevel
from pyspark.ml.evaluation import RegressionEvaluator


# === Spark job for LSH Cosine recommender ===
parser = argparse.ArgumentParser(description="Spark job for LSH Cosine recommender")

parser.add_argument("input_file", help="Path to input file")
parser.add_argument("-bucketLength", type=float, default=2.0, help="LSH parameter: bucket length (default: 2.0)")
parser.add_argument("-numHashTables", type=int, default=1, help="LSH parameter: number of hash tables (default: 1)")
parser.add_argument("-threshold", type=float, default=1.2, help="Recommendation similarity threshold (default: 1.2)")

args = parser.parse_args()

# Example usage:
# spark-submit deploy.py input.csv
# spark-submit deploy.py input.csv -bucketLength 1.5 -numHashTables 3 -threshold 0.8

input_file = args.input_file
bucket_length = args.bucketLength
num_hash_tables = args.numHashTables
threshold = args.threshold

saved_results_folder = "results"
saved_metrics_file = "results.csv"


# === Main Code ===
# create spark session
spark = SparkSession.builder \
    .appName("ItemItemCF") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .config("spark.sql.execution.useObjectHashAggregateExec", "false") \
    .config("spark.local.dir", "/tmp/spark") \
    .config("spark.shuffle.spill.compress", "true") \
    .config("spark.shuffle.compress", "true") \
    .getOrCreate()

# read the data
data = spark.read.csv(input_file, header=True, inferSchema=True) \
            .select("userId", "movieId", "rating")

# split in train and test
train, test = data.randomSplit([0.9, 0.1])
train = train.repartition(32).persist(StorageLevel.MEMORY_AND_DISK)
test = test.repartition(32).persist(StorageLevel.MEMORY_AND_DISK)

# start timer
start = time.time()

# create a user index to make sure its continuous
user_index = train.select("userId").distinct().withColumn(
    "user_idx",
    row_number().over(Window.orderBy("userId")) - 1
)
train = train.join(user_index, on="userId")

# get the similarities (check prototype for more details)
num_users = user_index.count()
def to_sparse_vector(user_ratings, size):
    sorted_pairs = sorted(user_ratings, key=lambda x: x.user_idx)
    indices = [x.user_idx for x in sorted_pairs]
    values = [x.rating for x in sorted_pairs]
    return Vectors.sparse(size, indices, values)
item_user = train.groupBy("movieId") \
    .agg(collect_list(struct("user_idx", "rating")).alias("user_ratings"))
item_vector_rdd = item_user.rdd.map(
    lambda row: Row(
        movieId=row["movieId"],
        features=to_sparse_vector(row["user_ratings"], num_users)
    )
)
item_vectors = spark.createDataFrame(item_vector_rdd).repartition(32).persist(StorageLevel.DISK_ONLY)
normalizer = Normalizer(inputCol="features", outputCol="norm_features", p=2.0)
normalized = normalizer.transform(item_vectors)
lsh = BucketedRandomProjectionLSH(
    inputCol="norm_features",
    outputCol="hashes",
    bucketLength=bucket_length,
    numHashTables=num_hash_tables
)
lsh_model = lsh.fit(normalized)
neighbors = lsh_model.approxSimilarityJoin(
    normalized,
    normalized,
    threshold=threshold,
    distCol="distance"
).filter(col("datasetA.movieId") < col("datasetB.movieId"))
neighbors_cosine = neighbors.withColumn(
    "cosine_sim",
    1 - (col("distance") ** 2) / 2
).select(
    col("datasetA.movieId").alias("movie_i"),
    col("datasetB.movieId").alias("movie_j"),
    "cosine_sim"
)
reverse = neighbors_cosine.selectExpr("movie_j as movie_i", "movie_i as movie_j", "cosine_sim")
similarities = neighbors_cosine.union(reverse)

# keep only the best neighbors for each movie
windowSpec = Window.partitionBy("movie_i").orderBy(col("cosine_sim").desc())
similarities = similarities.withColumn("rank", row_number().over(windowSpec)) \
                                  .filter(col("rank") <= 1) \
                                  .drop("rank")


similarities.write.option("header", "true").mode("overwrite").csv("23rexfzt.csv")



# get the predictions (check prototype for more details)
test_with_ratings = test.alias("t") \
    .join(similarities.alias("s"), col("t.movieId") == col("s.movie_i")) \
    .join(train.alias("r"), (col("t.userId") == col("r.userId")) & (col("s.movie_j") == col("r.movieId"))) \
    .select(
        col("t.userId"),
        col("t.movieId").alias("target_movie"),
        col("s.movie_j").alias("neighbor_movie"),
        col("s.cosine_sim"),
        col("r.rating").alias("neighbor_rating")
    )
predictions = test_with_ratings.groupBy("userId", "target_movie").agg(
    (sql_sum(col("cosine_sim") * col("neighbor_rating")) / sql_sum(col("cosine_sim"))).alias("pred_rating")
)
final = predictions.alias("p").join(
    test.alias("t"),
    (col("p.userId") == col("t.userId")) & (col("p.target_movie") == col("t.movieId")),
    how="right"
).select(
    col("t.userId"),
    col("t.movieId"),
    coalesce(col("p.pred_rating"), lit(3.0)).alias("pred_rating"),
    col("t.rating").alias("actual_rating")
).persist(StorageLevel.DISK_ONLY)
final.limit(1).collect() # wake up, don't be lazy

# evaluate the predictions (check prototype for more details)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="actual_rating", predictionCol="pred_rating")
rmse = evaluator.evaluate(final)
mae_evaluator = RegressionEvaluator(metricName="mae", labelCol="actual_rating", predictionCol="pred_rating")
mae = mae_evaluator.evaluate(final)

# end timer
end = time.time()


# === Save the results ===
# save predictions into a csv file
base_name = os.path.splitext(os.path.basename(input_file))[0]
final.write.option("header", "true").mode("overwrite").csv(f"{saved_results_folder}/{base_name}_predictions.csv")

# save metrics into other csv file
metrics = {"input_file": input_file,
           "time": end - start,
           "rmse": rmse,
           "mae": mae,
           "bucket_length": bucket_length,
           "num_hash_tables": num_hash_tables,
           "threshold": threshold,
           "timestamp": time.strftime("%Y%m%d_%H%M%S"), 
           }
new_row = pd.DataFrame([metrics])

if os.path.exists(saved_metrics_file):
    existing = pd.read_csv(saved_metrics_file)
    updated = pd.concat([new_row, existing], ignore_index=True)
else:
    updated = new_row

updated.to_csv(saved_metrics_file, index=False)


# === Clean up ===
spark.stop()
shutil.rmtree("/tmp/spark", ignore_errors=True)