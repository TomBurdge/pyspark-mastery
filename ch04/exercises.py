import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("Error")

# Exercise 4.1
# Ingest the following file successfully...
sample = spark.read.csv(
    "data/sample.csv", sep=",", header=True, quote="$", inferSchema=True
)
sample.show()

# Exercise 4.2
data = [{"item": "toilet_roll", "price": 2, "quantity": 3, "UPC": 4}]
df_4_1 = spark.createDataFrame(data)
print(df_4_1.drop("item", "UPC", "prices").columns)
print("Will be c. ['price', 'quantity']")

# Exercise 4.3
df_4_3 = spark.read.csv("data/BroadcastLogs_2018_Q3_M8-SAMPLE.csv")
df_4_3.show()
print(
    "The difference is pretty stark - only one string col with no header (inferred col name)"
)

# Exercise 4.4
# Create a new df, logs_clean, that contains columns that don't end with ID.
logs = spark.read.csv(
    os.path.join("data", "BroadcastLogs_2018_Q3_M8-SAMPLE.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)
logs_clean = logs.select([col for col in logs.columns if not col.endswith("ID")])

print("\n", logs_clean.columns)
