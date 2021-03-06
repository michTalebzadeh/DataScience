from __future__ import print_function
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
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
https://stackoverflow.com/questions/59278835/pyspark-how-to-write-dataframe-partition-by-year-month-day-hour-sub-directory
"""
wSpecY = Window().partitionBy(F.date_format('datetaken',"yyyy"))
wSpecM = Window().partitionBy(F.date_format('datetaken',"yyyy"), F.date_format('datetaken',"MM"))  ## partion by Year and Month

print(f"""\nAnnual House prices in {regionname} in GBP""")

house_df = spark.sql(f"""select * from {fullyQualifiedTableName} where regionname = '{regionname}'""")

df2 = house_df.filter(col("datetaken").between(f'{start_date}', f'{end_date}')). \
                select( \
                      F.date_format('datetaken','yyyy').alias('Year') \
                    , round(F.avg('averageprice').over(wSpecY)).alias('AVGPricePerYear') \
                    , round(F.avg('flatprice').over(wSpecY)).alias('AVGFlatPricePerYear') \
                    , round(F.avg('TerracedPrice').over(wSpecY)).alias('AVGTeraccedPricePerYear') \
                    , round(F.avg('SemiDetachedPrice').over(wSpecY)).alias('AVGSemiDetachedPricePerYear') \
                    , round(F.avg('DetachedPrice').over(wSpecY)).alias('AVGDetachedPricePerYear')). \
                distinct().orderBy('datetaken', asending=True)

df2.show(10,False)
df2.write.mode("overwrite").saveAsTable(f"""{v.DSDB}.yearlyhouseprices""")

wSpecP = Window().orderBy('year')

df_lag = df2.withColumn("prev_year_value", F.lag(df2['AVGPricePerYear']).over(wSpecP))
result = df_lag.withColumn('percent_change', F.when(F.isnull(df2.AVGPricePerYear - df_lag.prev_year_value),0). \
                           otherwise(F.round(((df2.AVGPricePerYear-df_lag.prev_year_value)*100.)/df_lag.prev_year_value,1)))

print(f"""\nPercent annual Average House prices change in {regionname}""")

rs = result.select('Year', 'AVGPricePerYear', 'prev_year_value', 'percent_change')
rs.show()
rs.write.mode("overwrite").saveAsTable(f"""{v.DSDB}.percenthousepricechange""")

print(f"""\nMonthly House prices in {regionname} in GBP""")

df3 = house_df.filter(col("datetaken").between('2018-01-01', '2020-01-01')). \
                select( \
                      col('datetaken')[1:7].alias('Year-Month') \
                    , round(F.avg('averageprice').over(wSpecM)).alias('AVGPricePerMonth') \
                    , round(F.avg('flatprice').over(wSpecM)).alias('AVGFlatPricePerMonth') \
                    , round(F.avg('TerracedPrice').over(wSpecM)).alias('AVGTeraccedPricePerMonth') \
                    , round(F.avg('SemiDetachedPrice').over(wSpecM)).alias('AVGSemiDetachedPricePerMonth') \
                    , round(F.avg('DetachedPrice').over(wSpecM)).alias('AVGDetachedPricePerMonth')). \
                distinct().orderBy('datetaken', asending=True)
df3.show(120,False)
df3.write.mode("overwrite").saveAsTable(f"""{v.DSDB}.monthlyhouseprices""")
