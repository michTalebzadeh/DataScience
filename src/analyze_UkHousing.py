from __future__ import print_function
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
import conf.variables as v
from sparkutils import sparkstuff as s
import usedFunctions as uf
from pyhive import hive
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import scatter_matrix
import six
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')
appName = "ukhouseprices"
spark = s.spark_session(appName)
spark.sparkContext._conf.setAll(v.settings)
sc = s.sparkcontext()
#
# Get data from Hive table
regionname = "Kensington and Chelsea"
tableName="ukhouseprices"
fullyQualifiedTableName = v.DSDB+'.'+tableName
start_date = "2010-01-01"
end_date = "2020-01-01"
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nStarted at");uf.println(lst)
if (spark.sql(f"""SHOW TABLES IN {v.DSDB} like '{tableName}'""").count() == 1):
    spark.sql(f"""ANALYZE TABLE {fullyQualifiedTableName} compute statistics""")
    rows = spark.sql(f"""SELECT COUNT(1) FROM {fullyQualifiedTableName}""").collect()[0][0]
    print("number of rows is ",rows)
else:
    print(f"""No such table {fullyQualifiedTableName}""")
    sys.exit(1)
f"""
rs1 = OracleDF.filter(col("datetaken").between("{start_date}","{end_date}")). \
        select(date_format(col("datetaken"),"EEEE").alias("DOW"), \
        date_format(col("datetaken"),"u").alias("DNUMBER"), \
        avg(col("weight")).over(wSpec).alias("weightkg"), \
        stddev(col("weight")).over(wSpec).alias("StandardDeviation"), \
        count(date_format(col("datetaken"),"EEEE")).over(wSpec).alias("sampleSize")).distinct().orderBy(col("weightkg").desc())

val rs1 = weights.filter('datetaken.between("2018-01-01","2018-12-31")).
        select(date_format('datetaken,"EEEE").as("DOW"),
        date_format('datetaken,"u").as("DNUMBER"),
        avg('weight).over(wSpec).as("weightkg"),
        stddev('weight).over(wSpec).as("StandardDeviation"),
        count(date_format('datetaken,"EEEE")).over(wSpec).as("sampleSize")).distinct.orderBy(desc("weightkg")):
val rs2= rs1.select(rank.over(wSpec2).as("Rank"),
             'DOW.as("DayofWeek"),
             'DNUMBER.cast("Integer").as("DayNumber"),
             round('weightkg,3).as("AverageForDayOfWeekInKg"),
             round('StandardDeviation,2).as("StandardDeviation"),
             'sampleSize.cast("Integer").as("sampleSizeInDays"))

https://stackoverflow.com/questions/59278835/pyspark-how-to-write-dataframe-partition-by-year-month-day-hour-sub-directory
"""
wSpecY = Window().partitionBy(F.date_format(col("datetaken"),"yyyy"))
wSpecM = Window().partitionBy(F.date_format(col("datetaken"),"yyyy"), F.date_format(col("datetaken"),"MM"))  ## partion by Year and Month
#val wSpec2 = Window.orderBy('weightkg)

print(f"""\nAnnual House prices in {regionname} in GBP""")

house_df = spark.sql(f"""select * from {fullyQualifiedTableName} where regionname = '{regionname}'""")

df2 = house_df.filter(col("datetaken").between('2010-01-01', '2020-01-01')). \
                select( \
                      F.date_format(col("datetaken"),"yyyy").alias("Year") \
                    , round(F.avg(col("averageprice")).over(wSpecY)).alias("AVGPricePerYear") \
                    , round(F.avg(col("flatprice")).over(wSpecY)).alias("AVGFlatPricePerYear") \
                    , round(F.avg(col("TerracedPrice")).over(wSpecY)).alias("AVGTeraccedPricePerYear") \
                    , round(F.avg(col("SemiDetachedPrice")).over(wSpecY)).alias("AVGSemiDetachedPricePerYear") \
                    , round(F.avg(col("DetachedPrice")).over(wSpecY)).alias("AVGDetachedPricePerYear")). \
                distinct().orderBy(col("datetaken"), asending=True)

df2.show(20,False)
df2.write.mode("overwrite").saveAsTable(f"""{v.DSDB}.yearlyhouseprices""")

print(f"""\nMonthly House prices in {regionname} in GBP""")

df3 = house_df.filter(col("datetaken").between('2018-01-01', '2020-01-01')). \
                select( \
                      col("datetaken")[1:7].alias("Year-Month") \
                    , round(F.avg(col("averageprice")).over(wSpecM)).alias("AVGPricePerMonth") \
                    , round(F.avg(col("flatprice")).over(wSpecM)).alias("AVGFlatPricePerMonth") \
                    , round(F.avg(col("TerracedPrice")).over(wSpecM)).alias("AVGTeraccedPricePerMonth") \
                    , round(F.avg(col("SemiDetachedPrice")).over(wSpecM)).alias("AVGSemiDetachedPricePerMonth") \
                    , round(F.avg(col("DetachedPrice")).over(wSpecM)).alias("AVGDetachedPricePerMonth")). \
                distinct().orderBy(col("datetaken"), asending=True)
df3.show(120,False)
df3.write.mode("overwrite").saveAsTable(f"""{v.DSDB}.monthlyhouseprices""")

