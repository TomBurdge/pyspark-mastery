from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("Error")

# Exercise 2.1
# Making the df in the question
data = [([1, 2, 3, 4, 5],), ([5, 6, 7, 8, 9, 10],)]
schema = ["numbers"]
exo_2_1_df = spark.createDataFrame(data, schema=schema)

print(
    """
The row will have 11 rows.
The author did a bit of a trick q by having 5 twice, nice.
      """
)
row_count = exo_2_1_df.select(F.explode(F.col("numbers"))).count()
print(f"There are {row_count} rows in the exploded df.")

# Exercise 2.2
# Given the following df, programmatically count the number of columns that *aren't* strings.

exo_2_2_df = spark.createDataFrame(
    [["test", "more test", 10_000_000_000], ["one", "two", "three"]]
)


def programmatic_string_count(df: DataFrame) -> int:
    """
    Takes a DataFrame as input and returns
    the count of columns that have a string data type.

    :param df: The parameter `df` is expected to be a DataFrame object
    :type df: DataFrame
    :return int: the count of columns in the DataFrame that have a data type of "string".
    """
    strings = [val for val in df.dtypes if val[1] == "string"]
    return len(strings)


print(f"The DataFrame has {programmatic_string_count(exo_2_2_df)} string columns.")

# Exercise 2.3
# Rewrite the following code snippet, remove the withColumnRenamed method.
# Which version is clearer and easier to read?

exo_2_3_df = (
    spark.read.text("data/pride-and-prejudice.txt")
    .select(F.length(F.col("value")))
    .withColumnRenamed("length(value)", "number_of_char")
)

# different way:
exo_2_3_df = (
    spark.read.text("data/pride-and-prejudice.txt")
    .withColumn("number_of_char", F.length(F.col("value")))
    .select("number_of_char")
)

# another way:
# this is probably the most readable
exo_2_3_df = spark.read.text("data/pride-and-prejudice.txt").select(
    F.length(F.col("value")).alias("number_of_char")
)

# Exercise 2.4
# Correct the error in the code

expo_2_4_df = spark.createDataFrame(
    [["key", 10_000, 20_000]], ["key", "value1", "value2"]
)
expo_2_4_df.printSchema()
# the problem was they hadn't selected key still.
# Best to either select the two cols or to withColumns
# i think the select is acc a bit more explicit
# and.. they hadn't kept the col names the same for max/maximum_value
try:
    exo2_4_mod = (
        expo_2_4_df.select(
            F.greatest(F.col("value1"), F.col("value2")).alias("max_value"), "key"
        )
        .select("key", "max_value")
        .show()
    )
except AnalysisException as err:
    print(err)

# Exercise 2.5
# getting the data in...
book = spark.read.text("data/pride-and-prejudice.txt")

lines = book.select(F.split(book.value, " ").alias("line"))

words = lines.select(F.explode(F.col("line")).alias("word"))

words_lower = words.select(F.lower(F.col("word")).alias("word_lower"))

words_clean = words_lower.select(
    F.regexp_extract(F.col("word_lower"), "[a-z]*", 0).alias("word")
)

words_nonull = words_clean.where(F.col("word") != "")

# a) remove all occurences of the word is.
words_nonis = words_nonull.where(F.col("word") != "is")

# b) Using the length function, kep only the words with more than three characters
words_greater_than_three_char = words_nonis.where(F.length(F.col("word")) > 3)

# Exercise 2.7
# this code is a simple fix - assigned the printSchema() object (I think these are usually NoneType) to book
# just remove that line
try:
    lines = book.select(F.split(book.value, " ").alias("line"))
    words = lines.select(F.explode(F.col("line")).alias("word"))
except AnalysisException as err:
    print(err)
