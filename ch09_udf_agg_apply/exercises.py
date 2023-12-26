import os

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("Error")
sc = spark.sparkContext

# Exercise 9.1
# IntegerType, pd.Series


# Exercise 9.2
def temp_to_temp(
    value: pd.Series, from_temp: str = "F", to_temp: str = "C"
) -> pd.Series:
    acceptable_vals = set(["C", "F", "K", "R"])
    if not set([from_temp, to_temp]).issubset(set(acceptable_vals)):
        return None

    def f_to_c(value: pd.Series) -> pd.Series:
        """
        The function `f_to_c` converts a temperature value from Fahrenheit to Celsius.

        :param value: The parameter "value" represents the temperature value in
        Fahrenheit that you want to convert to Celsius
        :return: The code is returning the result of the Fahrenheit to Celsius
        conversion.
        """
        return (value - 32) * 5 / 9

    def c_to_f(value: pd.Series) -> pd.Series:
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


# Exercise 9.3
def scale_temperature_C(temp_by_day: pd.DataFrame) -> pd.DataFrame:
    """Returns a simple normalization of the temperature for a site.

    If the temperature is constant for the whole window, defaults to 0.5
    """

    def f_to_c(temp):
        return (temp - 32.0) * 5.0 / 9.0

    temp = f_to_c(temp_by_day.temp)
    answer = temp_by_day[["stn", "year", "mo", "da", "temp"]]
    if temp.min() == temp.max():
        return answer.assign(temp_norm=0.5)

    return answer.assign(temp_norm=(temp - temp.min()) / (temp.max() - temp.min()))


# Exercise 9.4
source_dir = os.path.join("data", "Ch09")

gsod = (
    spark.read.parquet(source_dir)
    .dropna(subset=["year", "mo", "da", "temp"])
    .where(F.col("temp") != 9999.9)
    .drop("date")
    .groupby("year", "mo")
    .applyInPandas(
        scale_temperature_C,
        schema=["year string, mo string, temp double, temp_norm double"],
    )
)

# Don't care about sklearn in pyspark
