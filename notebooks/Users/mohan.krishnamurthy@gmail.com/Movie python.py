# Databricks notebook source exported at Thu, 26 May 2016 22:41:19 UTC
# Load the raw dataset stored as a CSV file
clickstreamRaw = sqlContext.read \
  .format("com.databricks.spark.csv") \
  .options(header="true", delimiter="\t", mode="PERMISSIVE", inferSchema="true") \
  .load("dbfs:///databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed")
  
# Convert the dataset to a more efficent format to speed up our analysis
clickstreamRaw.write \
  .mode("overwrite") \
  .format("parquet") \
  .save("/datasets/wiki-clickstream")

# COMMAND ----------

complete_dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'
small_dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest-small.zip'

# COMMAND ----------

# Load the raw dataset stored as a CSV file
clickstreamRaw = sqlContext.read \
  .format("com.databricks.spark.csv") \
  .options(header="true", delimiter="\t", mode="PERMISSIVE", inferSchema="true") \
  .load("dbfs:///databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed")
  
# Convert the dataset to a more efficent format to speed up our analysis
clickstreamRaw.write \
  .mode("overwrite") \
  .format("parquet") \
  .save("/datasets/wiki-clickstream")

# COMMAND ----------


import os

datasets_path = os.path.join('', 'datasets')

complete_dataset_path = os.path.join(datasets_path, 'ml-latest.zip')
small_dataset_path = os.path.join(datasets_path, 'ml-latest-small.zip')

# COMMAND ----------

small_ratings_raw_data = sc.textFile("s3://paid-qubole/default-datasets/gutenberg/pg20417.txt")
small_ratings_raw_data_header = small_ratings_raw_data.take(1)[0]


# COMMAND ----------

sc.version

# COMMAND ----------

sc.parallelize([1, 2, 3]).collect()

# COMMAND ----------

