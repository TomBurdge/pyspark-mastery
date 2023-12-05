import pyspark.sql.functions as F
import requests
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("Error")

# Exercise 3.1
# the second bullet point
# words_nonull.select(length(col("word").alias("length")).groupby("length").count())

# Exercise 3.2
# Why isn't the order preserved in the following code block?
# (
#     results.orderBy("count", ascending=False)
#     .groupby(F.length(F.col("word")))
#     .count()
#     .show(5)
# )

# the reason is because it goes out of order in the groupby
# it would still be ordered like this:
# (
#     results
#     .groupby(F.length(F.col("word")))
#     .count()
# .orderBy("count", ascending=False)
#     .show(5)
# )

# Exercise 3.3
# modify word count_submt.py to get the number of distinct words in Pride and Prejudice
# 1.
disinct_words = (
    spark.read.text("data/pride-and-prejudice.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), r"(\W+)?([a-z]+)", 2).alias("word"))
    .where(F.col("word") != "")
    .groupby(F.col("word"))
    .count()
    .count()
)

print(f"There are {disinct_words} distinct words in Jane Austen.")


# 2.
def distinct_words(df: DataFrame):
    return (
        df.select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word"))
        .select(F.regexp_extract(F.col("word"), r"(\W+)?([a-z]+)", 2).alias("word"))
        .where(F.col("word") != "")
        .groupby(F.col("word"))
        .count()
        .count()
    )


# getting another book from https....
url = "https://www.gutenberg.org/ebooks/72311.txt.utf-8"
response = requests.get(url)
text_content = response.text
book_df = spark.createDataFrame([(text_content,)], ["value"])
res = distinct_words(book_df)

print(
    f"""
      Here is a function finding distinct words for deep space scrolls:
      from project Guttenberg.
      There are {res} distinct words.
      (The exercise might have said reads from a path, but this is more fun
      to get from http.)
      """
)

# Exercise 3.4
# Taking word_count_submit.py, modify the script to return a sample of fie words
# that only appear once in Jane Austen's Pride and Prejudice.
five_single_appearance_words = (
    spark.read.text("data/pride-and-prejudice.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), r"(\W+)?([a-z]+)", 2).alias("word"))
    .where(F.col("word") != "")
    .groupby(F.col("word"))
    .count()
    .where(F.col("count") == 1)
    .select("word")
    .head(5)
)
print(
    f"""
    Five words that appear only once in Jane Austen:
    {five_single_appearance_words}.
    """
)

# Exercise 3.5
# 1 Use the substring function to return top 5 most popular first letters (in Jane Austen).
five_most_popular_first_letters = (
    spark.read.text("data/pride-and-prejudice.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), r"(\W+)?([a-z]+)", 2).alias("word"))
    .where(F.col("word") != "")
    .withColumn("substring", F.col("word").substr(1, 1))
    .groupby(F.col("substring"))
    .count()
    .orderBy("count", ascending=False)
    .select("substring")
    .head(5)
)
print(
    f"""
    The five most popular first letters in Jane Austen are:
    {five_most_popular_first_letters}.
"""
)

# 2 compute the number of words starting with a consonant or a vowel.
# (Hint: the isin() function might be useful)
vowel_consonant_counts = (
    spark.read.text("data/pride-and-prejudice.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), r"(\W+)?([a-z]+)", 2).alias("word"))
    .where(F.col("word") != "")
    .withColumn("substring", F.col("word").substr(1, 1))
    .withColumn(
        "vowel_or_consonant",
        F.when(F.col("substring").isin(["a", "e", "i", "o", "u"]), "vowel").otherwise(
            "consonant"
        ),
    )
    .groupby("vowel_or_consonant")
    .count()
)
vowel_consonant_counts.show()

# Exercise 3.6
# Say you want the count() and sum() of a GroupedData object.
# Why doesn't this work?
# It's because it has two columns (in the case of sum) - no longer grouped by and is doing over the already pivoted df
# according to book, covers how to do multiple aggregate applications in ch 4
# (from what I remember, this involves .agg() which can then take multiple)
