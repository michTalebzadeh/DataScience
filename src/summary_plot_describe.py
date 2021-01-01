from __future__ import print_function
import sys
from pyspark.sql.functions import col
from sparkutils import sparkstuff as s
import usedFunctions as uf
import matplotlib.pyplot as plt
from lmfit.models import LorentzianModel
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
start_date = "20100101"
end_date = "20200101"
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nStarted at");uf.println(lst)
# Model predictions
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
summary_df = spark.sql(f"""SELECT cast(date_format(datetaken, "yyyyMMdd") as int) as datetaken, flatprice, terracedprice, semidetachedprice, detachedprice FROM {summaryTableName}""")

df_10 = summary_df.filter(col("datetaken").between(f'{start_date}', f'{end_date}'))
#print(df_10.toPandas().columns.tolist())

# show pandas column list ['datetaken', 'flatprice', 'terracedprice', 'semidetachedprice', 'detachedprice']
p_dfm = df_10.toPandas()  # converting spark DF to Pandas DF

# Non-Linear Least-Squares Minimization and Curve Fitting

model = LorentzianModel()

n = len(p_dfm.columns)
for i in range(n):
  if p_dfm.columns[i] != 'datetaken':   # datetaken is x axis
     print(p_dfm.columns[i])
     params = model.guess(p_dfm[p_dfm.columns[i]], x=p_dfm['datetaken'])
     result = model.fit(p_dfm[p_dfm.columns[i]], params, x=p_dfm['datetaken'])
     result.plot_fit()
     print(result.fit_report())
plt.show()
plt.close()
sys.exit(0)
p_df = df_10.toPandas().describe()
axs = scatter_matrix(p_df, figsize=(10, 10))
# Describe returns a DF where count,mean, min, std,max... are values of the index
y = p_df.loc[['min', 'mean', 'max']]
#y = p_df.loc[['averageprice', 'flatprice']]
ax = y.plot(linewidth=2, colormap='jet', marker='.', markersize=20)
plt.grid(True)
plt.xlabel("UK House Price Index, January 2020", fontdict=v.font)
plt.ylabel("Property Prices in millions/£", fontdict=v.font)
plt.title(f"""Property price fluctuations in {regionname} for the past 10 years """, fontdict=v.font)
plt.legend(p_df.columns)
plt.show()
plt.close()
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nFinished at");uf.println(lst)
