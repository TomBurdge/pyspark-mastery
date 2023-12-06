import json

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("Error")

# Exercise 6.1
# Assume a json
data = json.dumps({"name": "sample name", "keywords": ["Pyspark", "Python", "Data"]})
# What is the schema once read by spark.read.json?
# It will be string and array with col names are keys
df_5_1 = spark.read.json(spark.sparkContext.parallelize([data]))
print(df_5_1.schema)
df_5_1.show()

# Exercise 6.3
# schema = T.StructType([T.StringType(), T.LongType(), T.LongType()])
# Structype doesn't take string etc. straight away
# need to first wrap the fields in T.StructField()
schema = T.StructType(
    [
        T.StructField("name1", T.StringType()),
        T.StructField("name2", T.LongType()),
        T.StructField("name3", T.LongType()),
    ]
)

# Exercise 6.4
# Why is it a bad idea to use the period or the square bracket
# in a column name, given that you also use it to reach hierarchical
# entities within a DataFrame?

# Exerise 6.5
print(
    spark.createDataFrame(
        [{"one": 1, "two": [1, 2, 3]}],
        schema=T.StructType(
            [
                T.StructField("one", T.LongType(), True),
                T.StructField("two", T.ArrayType(T.LongType(), True), True),
            ]
        ),
    )
)

# Exercise 6.6
three_shows = (
    spark.read.json("data/shows-*.json", multiLine=True)
    .select(
        "name",
        F.array_max("_embedded.episodes.airdate").cast("date").alias("last"),
        F.array_min("_embedded.episodes.airdate").cast("date").alias("first"),
    )
    .select("name", (F.col("last") - F.col("first")).alias("tenure"))
)

three_shows.show()


# Exercise 6.7
# Take the shows df and extract the air date and name of each episode
# in two array columns.
shows = spark.read.json("data/shows-silicon-valley.json").select(
    F.col("_embedded.episodes.airdate"),
    F.col("_embedded.episodes.name"),
)
shows.show()

# Exercise 6.8
# Given the following df, create a new df that contains a single
# map from one to square
exo6_8 = spark.createDataFrame([[1, 2], [2, 3], [3, 9]], ["one", "square"])
exo6_8.groupby().agg(
    F.collect_list("one").alias("one"), F.collect_list("square").alias("square")
).select(F.map_from_arrays("one", "square")).show(truncate=False)
