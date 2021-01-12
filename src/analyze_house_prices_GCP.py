from __future__ import print_function
import sys
import os

import logger
import findspark
findspark.init()
from pyspark.sql import functions as F
import sparkstuff as s
from othermisc import usedFunctions as uf
try:
  import variables as v
except ModuleNotFoundError:
  from conf import variables as v
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from google.cloud import bigquery
from google.oauth2 import service_account

appName = "DS"
spark = s.spark_session(appName)
sc = s.sparkcontext()

lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nStarted at");uf.println(lst)

tmp_bucket = "tmp_storage_bucket/tmp"

# Set the temporary storage location
spark.conf.set("temporaryGcsBucket",tmp_bucket)
spark.sparkContext.setLogLevel("ERROR")

HadoopConf = sc._jsc.hadoopConfiguration()
HadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
HadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

#bq = spark._sc._jvm.com.samelamin.spark.bigquery.BigQuerySQLContext(spark._wrapped._jsqlContext)

# needed filters

start_date = "2010-01-01"
end_date = "2020-01-01"

spark.conf.set("GcpJsonKeyFile",v.jsonKeyFile)
spark.conf.set("BigQueryProjectId",v.projectId)
spark.conf.set("BigQueryDatasetLocation",v.datasetLocation)
spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set("fs.gs.project.id", v.projectId)
spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
spark.conf.set("temporaryGcsBucket", v.tmp_bucket)

sqltext = ""
from pyspark.sql.window import Window

# read data from the Bigquery table in staging area
print("\nreading data from "+v.projectId+":"+v.inputTable)

summary_df = spark.read. \
              format("bigquery"). \
              option("credentialsFile",v.jsonKeyFile). \
              option("project", v.projectId). \
              option("parentProject", v.projectId). \
              option("dataset", v.sourceDataset). \
              option("table", v.sourceTable). \
              option("temporaryGcsBucket", v.tmp_bucket). \
    load()

summary_df.printSchema()
rows = summary_df.count()
print("Total number of rows for Kensington and Chelsea is ", rows)
summary_df.select("*").show(1,False)
#summary_df.show(2,False)
wSpecY = Window().partitionBy(F.date_format('date',"yyyy"))
df2 = summary_df. \
                select( \
                      F.date_format(F.col("date"),'yyyy').alias('Year') \
                    , F.round(F.avg(F.col("averageprice")).over(wSpecY)).alias('AVGPricePerYear') \
                    , F.round(F.avg('flatprice').over(wSpecY)).alias('AVGFlatPricePerYear') \
                    , F.round(F.avg('TerracedPrice').over(wSpecY)).alias('AVGTerracedPricePerYear') \
                    , F.round(F.avg('SemiDetachedPrice').over(wSpecY)).alias('AVGSemiDetachedPricePerYear') \
                    , F.round(F.avg('DetachedPrice').over(wSpecY)).alias('AVGDetachedPricePerYear')). \
                distinct().orderBy('date', asending=True)
df2.show(10,False)

"""
# Save the result set to a BigQuery table. Table is created if it does not exist
print("\nsaving data to " + v.fullyQualifiedoutputTableId)

summary_df. \
    write. \
    format("bigquery"). \
    option("temporaryGcsBucket", v.tmp_bucket). \
    option("table", v.outputTable). \
    mode("append"). \
    save()
summary_df. \
write. \
format("bigquery"). \
mode("overwrite"). \
option("table", v.fullyQualifiedoutputTableId). \
option("temporaryGcsBucket", v.tmp_bucket). \
save()
"""

lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nFinished at");uf.println(lst)

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars c:\jars\spark-bigquery-with-dependencies_2.12-0.18.0.jar --packages com.github.samelamin:spark-bigquery_2.11:0.2.6 analyze_house_prices_GCP.py"
)

#if __name__ == "__main__":
#  print("\n working on this code")
#  a = Class1
#  a.main()