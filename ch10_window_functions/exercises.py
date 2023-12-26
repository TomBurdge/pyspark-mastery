import glob
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("Error")
sc = spark.sparkContext

source_dir = os.path.join("data", "Ch09")

gsod = (
    spark.read.parquet(source_dir)
    .dropna(subset=["year", "mo", "da", "temp"])
    .where(F.col("temp") != 9999.9)
    .drop("date")
)

# Ex 10.1
# The answer is C
sol10_1 = Window.partitionBy("year", "mo", "da")
ex_10_1 = (
    gsod.select(
        "stn",
        "year",
        "mo",
        "da",
        "temp",
        F.max("temp").over(sol10_1).alias("max_this_day"),
    )
    .where(F.col("temp") == F.col("max_this_day"))
    .drop("temp")
)

# Ex 10.2
exo10_2 = spark.createDataFrame([[x // 4, 2] for x in range(1001)], ["index", "value"])
# reminder - is chaining the windowing here by putting an order which puts a partition on the windows
sol10_2 = Window.partitionBy("index").orderBy("value")
# has bucketing (as best it can) by 3 over the two partitions
exo10_2.withColumn("10_2", F.ntile(3).over(sol10_2)).orderBy(["index", "value"]).show()

# Ex 10.3
exo10_3 = spark.createDataFrame([[x] for x in range(1001)], ["ord"])
# first is 3,4,and then 5....until the end where goes 4,3
# bc is just 5 (a lit) and then running out of range at the edges of the df

# second is 10001 bc it captures all the values of the value column that is being calculated
# everything in the column is between -2 and 2 different from the val - bc is all lit 10
exo10_3.withColumn(
    "q_1", F.count("ord").over(Window.orderBy("ord").rowsBetween(-2, 2))
).show()

exo10_3.withColumn(
    "q_2", F.count("ord").over(Window.orderBy("ord").rangeBetween(-2, 2))
).show()

# Ex 10.4
files = glob.glob(os.path.join(source_dir, "*.parquet"))

gsod = (
    spark.read.parquet(files[0])
    .dropna(subset=["year", "mo", "da", "temp"])
    .where(F.col("temp") != 9999.9)
    .drop("date")
)

each_year = Window.partitionBy("year")

(
    gsod.withColumn(
        "min_temp",
        F.min("temp").over(each_year),
    )
    .where("temp = min_temp")
    .select(["year", "mo", "da", "stn", "temp"])
    .orderBy("year", "mo", "da")
    .show()
)

(
    gsod.withColumn(
        "max_temp",
        F.max("temp").over(each_year),
    )
    .where("temp = max_temp")
    .select(["year", "mo", "da", "stn", "temp"])
    .orderBy("year", "mo", "da")
    .show()
)
# if filtering and multiple records with the highest temp, will show them all

(
    gsod.withColumn(
        "avg_temp",
        F.avg("temp").over(each_year),
    )
    .select(["year", "avg_temp"])
    .dropDuplicates()
    .show()
)

# Ex 10.5
temp_per_month_asc = Window.partitionBy("mo").orderBy("count_temp")
temp_per_month_rnk = Window.partitionBy("mo").orderBy("count_temp", "row_tmp")
gsod.select(["stn", "year", "mo", "da", "temp", "count_temp"]).withColumn(
    "row_tmp", F.row_number().over(temp_per_month_asc)
).withColumn("rank_tpm", F.rank().over(temp_per_month_rnk)).show()

# Ex 10.6
# pretty cool - max tempurature in a 7 day rolling window
# range could be more readable but otherwise is ok
seven_days = (
    Window.partitionBy("stn")
    .orderBy("dtu")
    .rangeBetween(-7 * 60 * 24, 7 * 60 * 60 * 24)
)
(
    gsod.select(
        "stn", (F.to_date(F.concat_ws("-", "year", "mo", "da"))).alias("dt"), "temp"
    )
    .withColumn("dtu", F.unix_timestamp("dt").alias("dtu"))
    .withColumn("max_temp", F.max("temp").over(seven_days))
    # .where(F.col("temp") == F.col("max_temp"))
    .orderBy(["stn", "dt"])
    .show()
)

# 10.7
# Cool solution needed from the answers
# assume always 12 months in a year, can use the year number * 12 + the month number
# then partition on that
one_month_before_and_after = (
    Window.partitionBy("year").orderBy("num_mo").rangeBetween(-1, 1)
)
gsod.drop("dt", "dt_num").withColumn(
    "num_mo", F.col("year").cast("int") * 12 + F.col("mo").cast("int")
).withColumn("avg_count", F.avg("count_temp").over(one_month_before_and_after)).show()
