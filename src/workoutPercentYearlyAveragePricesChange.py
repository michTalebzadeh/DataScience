from __future__ import print_function
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')
try:
  import variables as v
except ModuleNotFoundError:
  from conf import parameters as v
from config import config

def main():
    regionname = sys.argv[1]  ## parameter passed
    short = regionname.replace(" ", "").lower()
    appName = config['common']['appName']
    spark = s.spark_session(appName)
    spark = s.setSparkConfHive(spark)

    #spark.sparkContext._conf.setAll(v.settings)
    sc = s.sparkcontext()
    #
    # Get data from Hive table
    tableName="yearlyaveragepricesalltable"
    yearlyAveragePricesTable = config['hiveVariables']['DSDB']+f""".{tableName}"""
    start_date = "2010"
    end_date = "2020"
    yearTable = config['hiveVariables']['DSDB']+f""".percentyearlyhousepricechange_{short}"""
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    print (f"""Getting Yearly percentages tables for {regionname}""")
    if (spark.sql(f"""SHOW TABLES IN {config['hiveVariables']['DSDB']} like '{tableName}'""").count() == 1):
        rows = spark.sql(f"""SELECT COUNT(1) FROM {yearlyAveragePricesTable}""").collect()[0][0]
        print("Total number of rows is ",rows)
    else:
        print(f"""No such table {yearlyAveragePricesTable}""")
        sys.exit(1)
    f"""
    https://stackoverflow.com/questions/59278835/pyspark-how-to-write-dataframe-partition-by-year-month-day-hour-sub-directory
    """
    house_df = spark.sql(f"""select * from {yearlyAveragePricesTable} where year BETWEEN {start_date} AND {end_date} and lower(regionname) = lower('{regionname}')""")
    rows = spark.sql(f"""SELECT COUNT(1) FROM {yearlyAveragePricesTable} where year BETWEEN {start_date} AND {end_date} and lower(regionname) = lower('{regionname}')""").collect()[0][0]
    print(f"Total number of rows for {regionname} is ", rows)
    wSpecPY = Window().orderBy('regionname','Year')

    df_lagY = house_df.withColumn("prev_year_value", F.lag(house_df['AVGPricePerYear']).over(wSpecPY))
    resultY = df_lagY.withColumn('percent_change', F.when(F.isnull(house_df.AVGPricePerYear - df_lagY.prev_year_value),0). \
                             otherwise(F.round(((house_df.AVGPricePerYear-df_lagY.prev_year_value)*100.)/df_lagY.prev_year_value,1)))
    print(f"""\nYear House price changes in {regionname} in GBP""")

    rsY = resultY.select('Year', 'AVGPricePerYear', 'prev_year_value', 'percent_change')
    rsY.show(36,False)
    rsY.write.mode("overwrite").saveAsTable(f"""{yearTable}""")

    print(f"""Created {yearTable}""")
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)

if __name__ == "__main__":
  print("\nworking on this code")
  main()
