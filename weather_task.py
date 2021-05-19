#!/usr/bin/env python
# coding: utf-8

# Import necessary package to access the underline libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DecimalType, TimestampType
from pyspark.sql.functions import col

# Creating simply a entry point for the program
spark = SparkSession.builder.getOrCreate()

# Read the raw data from a folder. Please "change the folder" based on the local environment.
df = spark.read.csv("/Users/DSS/_RawData/_Source/*.csv", header=True, sep=",")

# Change the data type for the columns
df = df.withColumn("ObservationDate_date", col("ObservationDate").cast(DateType())) \
       .withColumn("ScreenTemperature_decimal", col("ScreenTemperature").cast(DecimalType(5,2)))

df = df.drop("ObservationDate", "ScreenTemperature")
df = df.withColumnRenamed("ObservationDate_date","ObservationDate")          \
       .withColumnRenamed("ScreenTemperature_decimal","ScreenTemperature")

# Convert the incoming .csv file to parquet file
df.write.mode('overwrite').parquet("/Users/DSS/_RawData/_target")

# Read the converted parquet file.
parqDF = spark.read.parquet("/Users/DSS/_RawData/_target")
parqDF.createOrReplaceTempView("tblweather")


parkSQL = spark.sql("SELECT ObservationDate, ScreenTemperature, Region "
                    "FROM tblweather WHERE ScreenTemperature IN ("
                    "SELECT MAX(ScreenTemperature) From tblweather ) ")


parkSQL.show(truncate=False)