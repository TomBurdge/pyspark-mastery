import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("Error")

# Exercise 5.1
# Assume two tables, left and right, each containing a column named my_column.
# What is the result of this code?

# one = left.join(right, how="left_semi", on="my_columng")
# two = left.join(right, how="left_anti", on="my_column")
# one.union(two)
# the answer - it is the same as left

# Exercise 5.2
# Assume two dfs, red and blue.
# Which is the appropriate join to use in red.join(blue,...)
# If you want to join red and blue and keep all the records satisfying the predicate?
# answer - Inner join

# Exercise 5.3
# Assume two dfs, red and blue.
# Which is the appropriate join to use in red.join(blue,...)
# If you want to join red and blue and keep all the records satisfying the predicate?
# *And* the records in the blue table?
# Answer - Right

# Exercise 5.4
# Write PySpark code that will return the result of the following code
# block without using a left anti-join
# the code:
# left.join(right, how="left_anti", on="my_column").select("my_column").distinct()
# another way
# left.join(right, how="left", on=right["my_column"] == left["my_column"]).where(
#     right["my_column"].isnull()
# ).select(left["my_column"]).distinct()
# This was a good one i struggled with ^ and had to look at the answer.
# Helpful conceptually though.
# left anti is like the questions "Give me the users with no friends".

# Exercie 5.5
# Using the data from Call_signs.csv add Undertaking_name
# to the findal table
# to give a human-readable description of the channel
DIRECTORY = "data"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8-SAMPLE.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

log_identifier = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

cd_category = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_Category.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "CategoryID",
    "CategoryCD",
    F.col("EnglishDescription").alias("Category_Description"),
)

cd_program_class = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_ProgramClass.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "ProgramClassID",
    "ProgramClassCD",
    F.col("EnglishDescription").alias("ProgramClass_Description"),
)

###############################################################################
# Data processing
###############################################################################

logs = logs.drop("BroadcastLogID", "SequenceNO")

logs = logs.withColumn(
    "duration_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
)

# log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)

logs_and_channels = logs.join(log_identifier, "LogServiceID")

full_log = logs_and_channels.join(cd_category, "CategoryID", how="left").join(
    cd_program_class, "ProgramClassID", how="left"
)

answer = (
    full_log.groupby("LogIdentifierID")
    .agg(
        F.sum(
            F.when(
                F.trim(F.col("ProgramClassCD")).isin(
                    ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]
                ),
                F.col("duration_seconds"),
            ).otherwise(0)
        ).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total"),
    )
    .withColumn(
        "commercial_ratio", F.col("duration_commercial") / F.col("duration_total")
    )
    .fillna(0)
)

answer


df_5_5 = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables", "Call_Signs.csv"),
    sep=",",
    quote='"',
    header=True,
    inferSchema=True,
)

answer.join(df_5_5, how="left", on="LogIdentifierID").orderBy(
    "commercial_ratio", ascending=False
).show(20, False)
