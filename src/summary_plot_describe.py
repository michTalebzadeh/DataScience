from __future__ import print_function
import sys
from pyspark.sql.functions import col
from sparkutils import sparkstuff as s
import usedFunctions as uf
import matplotlib.pyplot as plt
from lmfit.models import LorentzianModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.stat import ChiSquareTest
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pandas.plotting import scatter_matrix
from scipy.optimize import differential_evolution
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
start_date = "2010"
end_date = "2020"
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nStarted at");uf.println(lst)
# Model predictions
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
#summary_df = spark.sql(f"""SELECT cast(date_format(datetaken, "yyyyMM") as int) as datetaken, flatprice, terracedprice, semidetachedprice, detachedprice FROM {summaryTableName}""")
summary_df = spark.sql(f"""SELECT cast(Year as int) as year, AVGFlatPricePerYear, AVGTerracedPricePerYear, AVGSemiDetachedPricePerYear, AVGDetachedPricePerYear FROM {v.DSDB}.yearlyhouseprices""")
df_10 = summary_df.filter(col("year").between(f'{start_date}', f'{end_date}'))
print(df_10.toPandas().columns.tolist())

# show pandas column list ['Year', 'AVGPricePerYear', 'AVGFlatPricePerYear', 'AVGTerracedPricePerYear', 'AVGSemiDetachedPricePerYear', 'AVGDetachedPricePerYear']
p_dfm = df_10.toPandas()  # converting spark DF to Pandas DF

# Non-Linear Least-Squares Minimization and Curve Fitting
import matplotlib.dates as mdates
model = LorentzianModel()
n = len(p_dfm.columns)
for i in range(n):
  if p_dfm.columns[i] != 'year':   # year is x axis in integer
     # it goes through the loop and plots individual average curves one by one and then prints a report for each y value
     vcolumn = p_dfm.columns[i]
     print(vcolumn)
     params = model.guess(p_dfm[vcolumn], x = p_dfm['year'])
     result = model.fit(p_dfm[vcolumn], params, x = p_dfm['year'])
     result.plot_fit()


     # do linear regression here
     # Prepare data for Machine Learning.And we need two columns only — features and label(p_dfm.columns[i]]):
     inputCols = ['year']
     vectorAssembler = VectorAssembler(inputCols=inputCols, outputCol='features')
     vhouse_df = vectorAssembler.transform(df_10)
     vhouse_df = vhouse_df.select(['features', 'AVGFlatPricePerYear'])
     vhouse_df.show(20)
     if vcolumn == "AVGFlatPricePerYear":
         plt.xlabel("Year", fontdict=v.font)
         plt.ylabel("Flat house prices in millions/GBP", fontdict=v.font)
         plt.title(f"""Flat price fluctuations in {regionname} for the past 10 years """, fontdict=v.font)
         plt.text(0.35,
            0.45,
            "Best-fit based on Non-Linear Lorentzian Model",
            transform=plt.gca().transAxes,
            color="grey",
            fontsize=10
         )
         print(result.fit_report())
         plt.xlim(left=2009)
         plt.xlim(right=2022)
         plt.show()
         plt.close()
     elif vcolumn == "AVGTerracedPricePerYear":
            plt.xlabel("Year", fontdict=v.font)
            plt.ylabel("Terraced house prices in millions/GBP", fontdict=v.font)
            plt.title(f"""Terraced house price fluctuations in {regionname} for the past 10 years """, fontdict=v.font)
            plt.text(0.35,
                     0.45,
                     "Best-fit based on Non-Linear Lorentzian Model",
                     transform=plt.gca().transAxes,
                     color="grey",
                     fontsize=10
                     )
            print(result.fit_report())
            plt.show()
            plt.close()
     elif vcolumn == "AVGSemiDetachedPricePerYear":
            plt.xlabel("Year", fontdict=v.font)
            plt.ylabel("semi-detached house prices in millions/GBP", fontdict=v.font)
            plt.title(f"""semi-detached house price fluctuations in {regionname} for the past 10 years """, fontdict=v.font)
            plt.text(0.35,
                     0.45,
                     "Best-fit based on Non-Linear Lorentzian Model",
                     transform=plt.gca().transAxes,
                     color="grey",
                     fontsize=10
                     )
            print(result.fit_report())
            plt.show()
            plt.close()
     elif vcolumn == "AVGDetachedPricePerYear":
            plt.xlabel("Year", fontdict=v.font)
            plt.ylabel("detached house prices in millions/GBP", fontdict=v.font)
            plt.title(f"""detached house price fluctuations in {regionname} for the past 10 years """, fontdict=v.font)
            plt.text(0.35,
                     0.45,
                     "Best-fit based on Non-Linear Lorentzian Model",
                     transform=plt.gca().transAxes,
                     color="grey",
                     fontsize=10
                     )
            print(result.fit_report())
            plt.show()
            plt.close()


p_df = df_10.select('AVGFlatPricePerYear','AVGTerracedPricePerYear','AVGSemiDetachedPricePerYear','AVGDetachedPricePerYear').toPandas().describe()
print(p_df)
#axs = scatter_matrix(p_df, figsize=(10, 10))
# Describe returns a DF where count,mean, min, std,max... are values of the index
y = p_df.loc[['min', 'mean', 'max']]
#y = p_df.loc[['averageprice', 'flatprice']]
ax = y.plot(linewidth=2, colormap='jet', marker='.', markersize=20)
plt.grid(True)
plt.xlabel("UK House Price Index, January 2020", fontdict=v.font)
plt.ylabel("Property Prices in millions/GBP", fontdict=v.font)
plt.title(f"""Property price fluctuations in {regionname} for the past 10 years """, fontdict=v.font)
plt.legend(p_df.columns)
plt.show()
plt.close()
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nFinished at");uf.println(lst)
