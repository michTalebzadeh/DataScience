from __future__ import print_function
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
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
try:
  import variables as v
except ModuleNotFoundError:
  from conf import variables as v

appName = "ukhouseprices"
spark = s.spark_session(appName)
spark.sparkContext._conf.setAll(v.settings)
sc = s.sparkcontext()
#
# Get data from Hive table
regionname = "Kensington and Chelsea"
tableName="ukhouseprices"
fullyQualifiedTableName = v.DSDB+'.'+tableName
summaryTableName = v.DSDB+'.'+'summary'
start_date = "2010-01-01"
end_date = "2020-01-01"
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nStarted at");uf.println(lst)
# Model predictions
import matplotlib.pyplot as plt
import seaborn as sns
import mpl_toolkits
import tkinter
import matplotlib.pyplot as plt
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
summary_df = spark.sql(f"""SELECT * FROM {v.DSDB}.yearlyhouseprices""")
p_df = summary_df.toPandas()
print(p_df)
#p_df.plot(kind='bar', x = 'Year', y = ['AVGPricePerYear', 'AVGFlatPricePerYear', 'AVGTerracedPricePerYear', 'AVGSemiDetachedPricePerYear', 'AVGDetachedPricePerYear'])
p_df.plot(kind='bar', stacked = False, x = 'Year', y = ['AVGFlatPricePerYear', 'AVGSemiDetachedPricePerYear', 'AVGDetachedPricePerYear'])
#ax = y.plot(linewidth=2, colormap='jet', marker='.', markersize=20)
plt.xlabel("year", fontdict=v.font)
plt.ylabel("Annual Property Prices per year in millions/GBP", fontdict=v.font)
#plt.title(f"""Annual property price fluctuations in {regionname} for the past 10 years """, fontdict=v.font)
plt.show()
plt.close()
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nFinished at");uf.println(lst)

