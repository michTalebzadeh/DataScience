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

def main():
    print (f"""Getting average yearly prices per region""")
    appName = "ukhouseprices"
    spark = s.spark_session(appName)
    spark.sparkContext._conf.setAll(v.settings)
    sc = s.sparkcontext()
    #
    # Get data from Hive table
    tableName="ukhouseprices"
    fullyQualifiedTableName = v.DSDB+'.'+tableName
    summaryTableName = v.DSDB+'.'+'summary'
    start_date = "2010-01-01"
    end_date = "2020-01-01"
    yearlyAveragePricesTable = v.DSDB+f""".yearlyaveragepricesTable"""
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    if (spark.sql(f"""SHOW TABLES IN {v.DSDB} like '{tableName}'""").count() == 1):
        spark.sql(f"""ANALYZE TABLE {fullyQualifiedTableName} compute statistics""")
        rows = spark.sql(f"""SELECT COUNT(1) FROM {fullyQualifiedTableName}""").collect()[0][0]
        print("Total number of rows is ",rows)
    else:
        print(f"""No such table {fullyQualifiedTableName}""")
        sys.exit(1)

    wSpecY = Window().partitionBy(F.date_format('datetaken',"yyyy"), 'regionname')

    house_df = spark.sql(f"""select * from {fullyQualifiedTableName}""")
    print(f"""\nAnnual House prices per regions in GBP""")
    # Workout yearly aversge prices
    df2 = house_df. \
                    select( \
                          F.date_format('datetaken','yyyy').alias('Year') \
                        , 'regionname' \
                        , round(F.avg('averageprice').over(wSpecY)).alias('AVGPricePerYear') \
                        , round(F.avg('flatprice').over(wSpecY)).alias('AVGFlatPricePerYear') \
                        , round(F.avg('TerracedPrice').over(wSpecY)).alias('AVGTerracedPricePerYear') \
                        , round(F.avg('SemiDetachedPrice').over(wSpecY)).alias('AVGSemiDetachedPricePerYear') \
                        , round(F.avg('DetachedPrice').over(wSpecY)).alias('AVGDetachedPricePerYear')). \
                    distinct().orderBy('datetaken', asending=True)

    df2.show(20,False)
    df2.write.mode("overwrite").saveAsTable(f"""{yearlyAveragePricesTable}""")
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)

if __name__ == "__main__":
  print("\nworking on this code")
  main()
