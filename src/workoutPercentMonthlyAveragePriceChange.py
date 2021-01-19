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
from config import config

def main():
    regionname = sys.argv[1]  ## parameter passed
    short = regionname.replace(" ", "").lower()
    appName = config['common']['appName']
    spark = s.spark_session(appName)
    spark = s.setSparkConfHive(spark)
    sc = s.sparkcontext()
    #
    # Get data from Hive table
    tableName=config['GCPVariables']['sourceTable']
    fullyQualifiedTableName = config['hiveVariables']['DSDB']+'.'+tableName
    summaryTableName = config['hiveVariables']['DSDB']+'.'+'summary'
    start_date = "2010-01-01"
    end_date = "2020-01-01"
    monthTable = config['hiveVariables']['DSDB']+f""".percentmonthlyhousepricechange_{short}"""
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    print(f"""Getting Monthly table for {regionname}""")
    if (spark.sql(f"""SHOW TABLES IN {config['hiveVariables']['DSDB']} like '{tableName}'""").count() == 1):
        spark.sql(f"""ANALYZE TABLE {fullyQualifiedTableName} compute statistics""")
        rows = spark.sql(f"""SELECT COUNT(1) FROM {fullyQualifiedTableName}""").collect()[0][0]
        print("Total number of rows is ",rows)
    else:
        print(f"""No such table {fullyQualifiedTableName}""")
        sys.exit(1)
    wSpecY = Window().partitionBy(F.date_format('datetaken',"yyyy"))

    house_df = spark.sql(f"""select * from {fullyQualifiedTableName} where lower(regionname) = lower('{regionname}')""")
    rows = spark.sql(f"""SELECT COUNT(1) FROM {fullyQualifiedTableName} where lower(regionname) = lower('{regionname}')""").collect()[0][0]
    print(f"Total number of rows for {regionname} is ", rows)

    wSpecM = Window().partitionBy(F.date_format('datetaken',"yyyy"), F.date_format('datetaken',"MM"))  ## partion by Year and Month

    df3 = house_df. \
                    select( \
                          (F.date_format(col('datetaken'), "yyyyMM")).alias('Year_Month') \
                        , round(F.avg('averageprice').over(wSpecM)).alias('AVGPricePerMonth') \
                        , round(F.avg('flatprice').over(wSpecM)).alias('AVGFlatPricePerMonth') \
                        , round(F.avg('TerracedPrice').over(wSpecM)).alias('AVGTerracedPricePerMonth') \
                        , round(F.avg('SemiDetachedPrice').over(wSpecM)).alias('AVGSemiDetachedPricePerMonth') \
                        , round(F.avg('DetachedPrice').over(wSpecM)).alias('AVGDetachedPricePerMonth')). \
                    distinct().orderBy('datetaken', asending=True)

    wSpecPM = Window().orderBy('Year_Month')

    df_lagM = df3.withColumn("prev_month_value", F.lag(df3['AVGPricePerMonth']).over(wSpecPM))
    resultM = df_lagM.withColumn('percent_change', F.when(F.isnull(df3.AVGPricePerMonth - df_lagM.prev_month_value),0). \
                             otherwise(F.round(((df3.AVGPricePerMonth-df_lagM.prev_month_value)*100.)/df_lagM.prev_month_value,1)))
    print(f"""\nMonthly House price changes in {regionname} in GBP""")
    rsM = resultM.select('Year_Month', 'AVGPricePerMonth', 'prev_month_value', 'percent_change')
    rsM.show(36,False)
    rsM.write.mode("overwrite").saveAsTable(f"""{monthTable}""")
    print(f"""Results saved in {monthTable}""")
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)

if __name__ == "__main__":
  print("\nworking on this code")
  main()
