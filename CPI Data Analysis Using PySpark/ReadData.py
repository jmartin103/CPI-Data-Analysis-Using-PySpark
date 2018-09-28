import pandas as pd
from pyspark.sql import SparkSession, SQLContext, Row
import numpy as np
from matplotlib import pyplot as plt
from matplotlib import style
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import *
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Read 2013 CPI Data
RDD2013_1 = sc.textFile("2013Data.csv")
print RDD2013_1.take(100)

# Extract lines with "All items"
RDD2013_2 = RDD2013_1.filter(lambda line: "All items," in line)
print RDD2013_2.take(100)

# Split data
RDD2013_3 = RDD2013_2.map(lambda line: line.split(','))
print RDD2013_3.take(100)

# Extract CPI for all items, and store in RDD
RDD2013_4 = RDD2013_3.map(lambda line: line[5])
print RDD2013_4.collect()

# Convert RDD to DataFrame
DF2013_1 = RDD2013_4.map(lambda cpi: (cpi, )).toDF()
DF2013_2 = DF2013_1.withColumnRenamed("_1", "2013 CPI")
DF2013_2.show()

# Calculate average CPI
DF2013_3 = DF2013_2.agg(avg("2013 CPI"))
DF2013_3.show()

# Calculate minimum CPI
DF2013_4 = DF2013_2.agg(min("2013 CPI"))
DF2013_4.show()

DF2013_5 = DF2013_2.agg(max("2013 CPI"))
DF2013_5.show()

RDD2014_1 = sc.textFile("2014Data.csv")
print RDD2014_1.take(100)

RDD2014_2 = RDD2014_1.filter(lambda line: "All items," in line)
print RDD2014_2.take(100)

RDD2014_3 = RDD2014_2.map(lambda line: line.split(','))
print RDD2014_3.take(100)

RDD2014_4 = RDD2014_3.map(lambda line: line[5])
print RDD2014_4.collect()

DF2014_1 = RDD2014_4.map(lambda cpi: (cpi, )).toDF()
DF2014_2 = DF2014_1.withColumnRenamed("_1", "2014 CPI")
DF2014_2.show()

DF2014_3 = DF2014_2.agg(avg("2014 CPI"))
DF2014_3.show()

DF2014_4 = DF2014_2.agg(min("2014 CPI"))
DF2014_4.show()

DF2014_5 = DF2014_2.agg(max("2014 CPI"))
DF2014_5.show()

RDD2015_1 = sc.textFile("2015Data.csv")
print RDD2015_1.take(100)

RDD2015_2 = RDD2015_1.filter(lambda line: "All items," in line)
print RDD2015_2.take(100)

RDD2015_3 = RDD2015_2.map(lambda line: line.split(','))
print RDD2015_3.take(100)

RDD2015_4 = RDD2015_3.map(lambda line: line[5])
print RDD2015_4.collect()

DF2015_1 = RDD2015_4.map(lambda cpi: (cpi, )).toDF()
DF2015_2 = DF2015_1.withColumnRenamed("_1", "2015 CPI")
DF2015_2.show()

DF2015_3 = DF2015_2.agg(avg("2015 CPI"))
DF2015_3.show()

DF2015_4 = DF2015_2.agg(min("2015 CPI"))
DF2015_4.show()

DF2015_5 = DF2015_2.agg(max("2015 CPI"))
DF2015_5.show()

RDD2016_1 = sc.textFile("2016Data.csv")
print RDD2016_1.take(100)

RDD2016_2 = RDD2016_1.filter(lambda line: "All items," in line)
print RDD2016_2.take(100)

RDD2016_3 = RDD2016_2.map(lambda line: line.split(','))
print RDD2016_3.take(100)

RDD2016_4 = RDD2016_3.map(lambda line: line[5])
print RDD2016_4.collect()

DF2016_1 = RDD2016_4.map(lambda cpi: (cpi, )).toDF()
DF2016_2 = DF2016_1.withColumnRenamed("_1", "2016 CPI")
DF2016_2.show()

DF2016_3 = DF2016_2.agg(avg("2016 CPI"))
DF2016_3.show()

DF2016_4 = DF2016_2.agg(min("2016 CPI"))
DF2016_4.show()

DF2016_5 = DF2016_2.agg(max("2016 CPI"))
DF2016_5.show()

RDD2017_1 = sc.textFile("2017Data.csv")
print RDD2017_1.take(100)

RDD2017_2 = RDD2017_1.filter(lambda line: "All items," in line)
print RDD2017_2.take(100)

RDD2017_3 = RDD2017_2.map(lambda line: line.split(','))
print RDD2017_3.take(100)

RDD2017_4 = RDD2017_3.map(lambda line: line[5])
print RDD2017_4.collect()

DF2017_1 = RDD2017_4.map(lambda cpi: (cpi, )).toDF()
DF2017_2 = DF2017_1.withColumnRenamed("_1", "2017 CPI")
DF2017_2.show()

DF2017_3 = DF2017_2.agg(avg("2017 CPI"))
DF2017_3.show()

DF2017_4 = DF2017_2.agg(min("2017 CPI"))
DF2017_4.show()

DF2017_5 = DF2017_2.agg(max("2017 CPI"))
DF2017_5.show()

# Combine all averages into one DataFrame
DFavg1 = DF2013_3.union(DF2014_3.union(DF2015_3.union(DF2016_3.union(DF2017_3))))
DFavg2 = DFavg1.withColumnRenamed("avg(2013 CPI)", "Averages (2013-2017)")
DFavg2.show()

# Convert PySpark dataframe to Pandas dataframe for plotting
DFavg3 = DFavg2.toPandas()
DFavg3.plot()

# Show plot of average CPI from 2013-2017
plt.title('Average CPI (2013-2017)')
plt.xlabel('Year')
plt.ylabel('Consumer Price Index (CPI)')
plt.show()
