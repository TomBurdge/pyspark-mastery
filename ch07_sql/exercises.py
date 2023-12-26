import os
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("Error")

# Exercise 7.1
# With the elements f, which are equivalent?
# d is equivalent (but returns an int), b is equivalent and shows a df

# Exercise 7.2

DATA_FILES = [
    "data_Q2_2019",
    "data_Q2_2019",
    "data_Q3_2019",
    "data_Q4_2019",
]

data = [
    spark.read.csv(os.path.join("data", "Ch07", file), header=True, inferSchema=True)
    for file in DATA_FILES
]


common_columns = list(
    reduce(lambda x, y: x.intersection(y), [set(df.columns) for df in data])
)

assert set(["model", "capacity_bytes", "date", "failure"]).issubset(set(common_columns))

full_data = reduce(
    lambda x, y: x.select(common_columns).union(y.select(common_columns)), data
)


full_data = full_data.selectExpr(
    "model", "capacity_bytes / pow(1024, 3) capacity_GB", "date", "failure"
)

summarized_data = (
    full_data.groupby("model", "capacity_GB")
    .agg(F.sum(F.col("failure")).alias("failures"), F.count("*").alias("drive_days"))
    .selectExpr("model", "capacity_GB", "failures / drive_days failure_rate")
)

# bonus - with only SQL
full_data.createOrReplaceTempView("full_data")

summarized_data = spark.sql(
    """
SELECT
    model,
    capacity_GB,
    SUM(failure) AS failures,
    COUNT(*) AS drive_days
FROM full_data
GROUP BY model, capacity_gb;
"""
)


# Exercise 7.3

ch_07 = (
    reduce(lambda x, y: x.select(common_columns).union(y.select(common_columns)), data)
    .selectExpr(
        "serial_number",
        "model",
        "capacity_bytes /  pow(1024,3) capacity_GB",
        "date",
        "failure",
    )
    .groupby("serial_number", "model", "capacity_GB")
    .agg(
        F.date_diff(F.max("date").cast("date"), F.min("date").cast("date")).alias("age")
    )
    .groupby("model", "capacity_GB")
    .agg(F.mean(F.col("age")).alias("avg_age"))
    .orderBy("avg_age", ascending=False)
)

# Exercise 7.4
# What is the total capacity (in TB) that backblaze records
# at the beginning of each month?

full_data = reduce(
    lambda x, y: x.select(common_columns).union(y.select(common_columns)), data
)

tb_per_month = (
    full_data.selectExpr(
        "capacity_bytes / 1e12 capacity_TB",
        "cast(date as date) as date",
    )
    .where("extract(day from date) = 1")
    .groupby("date")
    .agg(F.sum("capacity_TB"))
)

# Exercise 7.5
# with window function and not inefficient method
window_spec = Window().partitionBy("model")

most_common = (
    full_data.withColumn("count", F.count("capacity_bytes").over(window_spec))
    .withColumn(
        "most_common_combination",
        F.row_number().over(Window.partitionBy("model").orderBy(F.desc("count"))),
    )
    .filter("most_common_combination = 1")
    .drop("count", "most_common_combination")
)
