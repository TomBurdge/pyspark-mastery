from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("Error")
sc = spark.sparkContext

# Ex 11.1
# Would have 1 job and two stages
# no changes - these are optimised pyspark at lower level

# Ex 11.2
# no use of show - which has a limit

# Ex 11.3
# a and b narrow, c d and e wide.
