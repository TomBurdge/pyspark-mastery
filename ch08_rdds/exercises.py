from fractions import Fraction
from typing import Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("Error")
sc = spark.sparkContext

# Ex 8.1
# Create a recreation of the count() function with RDDs
rdd = sc.parallelize(["b", "a", "c", "c"])
# uses reduceByKey but i don't see why shouldn't
recreated_res = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
assert recreated_res.collect() == [("b", 1), ("a", 1), ("c", 2)]

# Ex 8.2
# Should return a [1] because everything else evaluates to F in python.

# Ex 8.3
# Using the following definitions, create a temp_to_temp(value, from, to)
# that takes a numerical value in from degrees and converts it to degrees


def temp_to_temp(value: int = 0, from_temp: str = "F", to_temp: str = "C"):
    acceptable_vals = set(["C", "F", "K", "R"])
    if not set([from_temp, to_temp]).issubset(set(acceptable_vals)):
        return None

    def f_to_c(value):
        """
        The function `f_to_c` converts a temperature value from Fahrenheit to Celsius.

        :param value: The parameter "value" represents the temperature value in
        Fahrenheit that you want to convert to Celsius
        :return: The code is returning the result of the Fahrenheit to Celsius
        conversion.
        """
        return (value - 32) * 5 / 9

    def c_to_f(value):
        """
        The function `c_to_f` converts a temperature value from Celsius to Fahrenheit.

        :param value: The parameter "value" represents a temperature value in Celsius
        :return: the value converted from Celsius to Fahrenheit.
        """
        return value * 9.0 / 5.0 + 32.0

    K_OVER_C = 273.15
    R_OVER_F = 459.67

    if from_temp == "K":
        value -= K_OVER_C
        from_temp = "C"
    elif from_temp == "R":
        value -= R_OVER_F
        from_temp == "F"
    if from_temp == "C":
        if to_temp == "C":
            return value
        elif to_temp == "F":
            return c_to_f(value)
        elif to_temp == "K":
            return value + K_OVER_C
        elif to_temp == "R":
            return c_to_f(value) + R_OVER_F

    if from_temp == "F":
        if to_temp == "F":
            return value
        elif to_temp == "C":
            return f_to_c(value)
        elif to_temp == "K":
            return f_to_c(value) + K_OVER_C
        elif to_temp == "R":
            return value + R_OVER_F


# incomplete testing
assert temp_to_temp(0, "C", "F") == 32
assert temp_to_temp(32, "F", "C") == 0
assert temp_to_temp(0, "C", "K") == 273.15
assert temp_to_temp(0, "F", "R") == 459.67


# Ex 8.4
# correct the UDF to not generate an error
@F.udf(T.DoubleType())
def naive_udf(value: float) -> float:
    return value * 3.14159


# Create a DataFrame from the RDD
rdd = sc.parallelize([("Charlie", 1.0), ("Brown", 2.0), ("Is", 3.0), ("Clown", 4.0)])

df = rdd.toDF(["name", "value"])

# Apply the UDF to the DataFrame using 'withColumn'
result_df = df.withColumn("result", naive_udf("value"))

# Show the result
# result_df.show()

# Ex 8.5
"""
Create a UDF that adds two fractions together, and test it by adding the reduced_fraction
to itself in the test_frac dataframe.
"""

Frac = Tuple[int, int]


@F.udf(T.ArrayType())
def naive_udf(left: Frac, right: Frac) -> Optional[Frac]:
    left_num, left_denom = left
    right_num, right_denom = right
    if left_denom and right_denom:
        answer = Fraction(left_num, left_denom) + Fraction(right_num, right_denom)
        return answer.numerator, answer.denominator
    return None


# Ex 8.6
# simple chage - make a MAX_LONG and MIN_LONG which are te highest and lowest
# then just before end, check if more or less than and return None if so
# don't change type hints - Optional allows for returning None
