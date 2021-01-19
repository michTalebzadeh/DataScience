from __future__ import print_function
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import scatter_matrix
import locale
import mpl_toolkits
import tkinter
locale.setlocale(locale.LC_ALL, 'en_GB')
from config import config

def main():
  regionname = sys.argv[1]  ## parameter passed
  short = regionname.replace(" ", "").lower()
  appName = config['common']['appName']
  spark = s.spark_session(appName)
  spark = s.setSparkConfHive(spark)
  sc = s.sparkcontext()
  #
  start_date = 201001
  end_date = 202001
  tableName = f"""percentmonthlyhousepricechange_{short}"""
  monthTable = config['hiveVariables']['DSDB'] + "."+f"""{tableName}"""
  lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
  print("\nStarted at");uf.println(lst)
  print(f"""Getting plots for {regionname}""")
  spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

  if (spark.sql(f"""SHOW TABLES IN {config['hiveVariables']['DSDB']} like '{tableName}'""").count() == 1):
    spark.sql(f"""ANALYZE TABLE {monthTable} compute statistics""")
  else:
    print(f"""No such table {monthTable}""")
    sys.exit(1)
  summary_df = spark.sql(f"""select Year_Month, percent_change as PercentMonthlyChange FROM {monthTable} WHERE year_month BETWEEN {start_date} AND {end_date} ORDER BY 1""")
  p_df = summary_df.toPandas()
  print(p_df)
  p_df.plot(kind='bar',stacked = False, x = 'Year_Month', y = ['PercentMonthlyChange'])
  plt.xlabel("Year", fontdict=config['plot_fonts']['font'])
  plt.ylabel("Monthly Percent Property Price change", fontdict=config['plot_fonts']['font'])
  plt.title(f"""Property price fluctuations in {regionname} for the past 10 years """, fontdict=config['plot_fonts']['font'])
  plt.margins(0.15)
  plt.subplots_adjust(bottom=0.25)
  plt.show()
  plt.close()
  lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
  print("\nFinished at");uf.println(lst)

if __name__ == "__main__":
  print("\nworking on this code")
  main()
