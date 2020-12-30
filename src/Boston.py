from __future__ import print_function
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import conf.variables as v
from sparkutils import sparkstuff as s
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import numpy as np # linear algebra
import matplotlib.pyplot as plt
import pandas as pd    # data processing, exel sheet type for Python
from pandas.plotting import scatter_matrix
import six
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats

"""
What is the input data (all come lowercase in Boston.csv)
id  - this was not labelled or used in the analysis. however, it seems to be a sequence column in string format. So I called it id
crim — per capita crime rate by town.
zn — proportion of residential land zoned for lots over 25,000 sq.ft.
indus — proportion of non-retail business acres per town.
chas — Charles River dummy variable (= 1 if tract bounds river; 0 otherwise).
nox — nitrogen oxides concentration (parts per 10 million).
rm — average number of rooms per dwelling.
age — proportion of owner-occupied units built prior to 1940.
dis — weighted mean of distances to five Boston employment centres.
rad — index of accessibility to radial highways.
tax — full-value property-tax rate per $10,000.
ptratio — pupil-teacher ratio by town.
black — 1000(Bk — 0.63)² where Black is the proportion of blacks by town.
lstat — lower status of the population (percent).
medv — median value of owner-occupied homes in $1000s. This is the target variable

The input data set contains data about details of various houses. Based on the information provided, 
the goal is to come up with a model to predict median value of a given house in the area.

"""
appName = "model_to_predict_median_house_values"
spark =s.spark_session(appName)
rows = spark.read.csv(v.Boston_csvlocation, header="true").count()
print("\nnumber of rows is ",rows)
if (rows == 0):
         println("Empty CSV directory, aborting!")
         sys.exit(1)

house_df = spark.read.csv(v.Boston_csvlocation, header="true")

for col_name in house_df.columns:
    if(col_name == "chas" or col_name == "rad" or col_name == "tax"):
        house_df = house_df.withColumn(col_name, F.col(col_name).cast("integer"))
    else:
        house_df = house_df.withColumn(col_name, F.col(col_name).cast("double"))
house_df.printSchema()
#print(house_df.dtypes)
#print(house_df.columns)
print(house_df.describe().toPandas().transpose())
numeric_features = [t[0] for t in house_df.dtypes if t[1] == 'int' or t[1] == 'double']
sampled_data = house_df.select(numeric_features).sample(False, 0.8).toPandas()
axs = scatter_matrix(sampled_data, figsize=(10, 10))
n = len(sampled_data.columns)
for i in range(n):
    v = axs[i, 0]
    v.yaxis.label.set_rotation(0)
    v.yaxis.label.set_ha('right')
    v.set_yticks(())
    h = axs[n-1, i]
    h.xaxis.label.set_rotation(90)
    h.set_xticks(())

for col_name in house_df.columns:
    if not( isinstance(house_df.select(col_name).take(1)[0][0], six.string_types)):
        print( "Correlation to medv for ", col_name, house_df.stat.corr('medv',col_name))
inputCols = [ 'crim', 'zn', 'indus', 'chas', 'nox', 'rm', 'age', 'dis', 'rad', 'tax', 'ptratio', 'black', 'lstat']

# Prepare data for Machine Learning. And we need two columns only — features and label(“medv”):
vectorAssembler = VectorAssembler(inputCols = inputCols, outputCol = 'features')

vhouse_df = vectorAssembler.transform(house_df)
vhouse_df = vhouse_df.select(['features', 'medv'])
vhouse_df.show(3)

splits = vhouse_df.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

lr = LinearRegression(featuresCol = 'features', labelCol='medv', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

# Summarize the model over the training set and print out some metrics:

trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

train_df.describe().show()

lr_predictions = lr_model.transform(test_df)
lr_predictions.select("prediction","medv","features").show(5)
lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
                 labelCol="medv",metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

test_result = lr_model.evaluate(test_df)
print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)

print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()

predictions = lr_model.transform(test_df)
predictions.select("prediction","medv","features").show()

dt = DecisionTreeRegressor(featuresCol ='features', labelCol = 'medv')
dt_model = dt.fit(train_df)
dt_predictions = dt_model.transform(test_df)
dt_evaluator = RegressionEvaluator(
    labelCol="medv", predictionCol="prediction", metricName="rmse")
rmse = dt_evaluator.evaluate(dt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

dt_model.featureImportances

house_df.take(1)

gbt = GBTRegressor(featuresCol = 'features', labelCol = 'medv', maxIter=10)
gbt_model = gbt.fit(train_df)
gbt_predictions = gbt_model.transform(test_df)
gbt_predictions.select(col("prediction"), col("medv").alias("Median Value"), col("features")).show(5)

gbt_evaluator = RegressionEvaluator(
    labelCol="medv", predictionCol="prediction", metricName="rmse")
rmse = gbt_evaluator.evaluate(gbt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)