from __future__ import print_function
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
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
from config import config
import seaborn as sns
import mpl_toolkits
import tkinter

def main():
    regionname = sys.argv[1]  ## parameter passed
    short = regionname.replace(" ", "").lower()
    print(f"""Getting plots for {regionname}""")

    appName = config['common']['appName']
    spark = s.spark_session(appName)
    spark.sparkContext._conf.setAll(v.settings)
    sc = s.sparkcontext()
    #
    # Get data from Hive table
    tableName = config['GCPVariables']['sourceTable']
    fullyQualifiedTableName = config['hiveVariables']['DSDB']+'.'+tableName
    summaryTableName = config['hiveVariables']['DSDB']+'.'+'summary'
    start_date = "2010-01-01"
    end_date = "2020-01-01"
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    summary_df = spark.sql(f"""SELECT datetaken, salesvolume as volumeOfSales FROM {fullyQualifiedTableName} WHERE datetaken BETWEEN '{start_date}' AND '{end_date}' AND lower(regionname) = lower('{regionname}') ORDER BY datetaken""")
    p_df = summary_df.toPandas()
    print(p_df)
    # Describe returns a DF where count,mean, min, std,max... are values of the index
    p_df.plot(kind='scatter', stacked = False, x = 'datetaken', y = ['volumeOfSales'], colormap='jet')
    #ax = y.plot(linewidth=2, colormap='jet', marker='.', markersize=20)
    plt.xlabel("year", fontdict=config['plot_fonts']['font'])
    plt.ylabel("Volume of Monthly Sales", fontdict=config['plot_fonts']['font'])
    plt.title(f"""Stats from {regionname} for the past 10 years """, fontdict=config['plot_fonts']['font'] )
    plt.text(0.35,
             0.85,
             "2016 stamp duty change impact [Ref 1]",
             transform=plt.gca().transAxes,
             color="darkgreen",
             fontsize=10
             )
    plt.show()
    plt.close()
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)

if __name__ == "__main__":
  print("\nworking on this code")
  main()
