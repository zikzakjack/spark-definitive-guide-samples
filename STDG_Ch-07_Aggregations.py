# cd D:\zikzakjack\code\git\Spark-The-Definitive-Guide
# pyspark --master local[*] --driver-memory 10G
# spark.sparkContext.setLogLevel("ERROR")

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *


df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("data/retail-data/all/*.csv")\
  .coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")

df.count()

# DataFrame-level aggregations
count()
countDistinct()
approx_count_distinct()
min()
max()
sum()
avg()
mean()
variance()
var_samp()
var_pop()
stddev()
stddev_samp()
stddev_pop()
skewness()
kurtosis
corr()
covar_samp()
covar_pop()

# Aggregation Functions
df.select(count("StockCode")).show() # 541909
df.select(count(col("stockcode"))).show()
df.select(count(column("stockcode"))).show()
df.select(count(expr("stockcode"))).show()
df.select(count(lit(1))).show() # 541909
# SELECT COUNT(*) FROM dfTable

df.select(countDistinct("StockCode")).show() # 4070
df.select(countDistinct(col("StockCode"))).show() # 4070
# SELECT COUNT(DISTINCT *) FROM DFTABLE

# maximum estimation error allowed -> 0.1
df.select(approx_count_distinct("StockCode", 0.1)).show() # 3364
# SELECT approx_count_distinct(StockCode, 0.1) FROM DFTABLE

# first and last
df.select(first("StockCode"), last("StockCode")).show()
# SELECT first(StockCode), last(StockCode) FROM dfTable

# min and max
df.select(min("Quantity"), max("Quantity")).show()
# SELECT min(Quantity), max(Quantity) FROM dfTable

# sum
df.select(sum("Quantity")).show() # 5176450
# SELECT sum(Quantity) FROM dfTable

# sumDistinct
df.select(sumDistinct("Quantity")).show() # 29310

# avg
df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))\
  .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()

# Variance and Standard Deviation
# measures of the spread of the data around the mean
# The variance is the average of the squared differences from the mean
# the standard deviation is the square root of the variance.
df.select(variance("Quantity"), var_samp("Quantity"), var_pop("Quantity"), stddev("Quantity"), stddev_samp("Quantity"), stddev_pop("Quantity")).show()
# SELECT var_pop(Quantity), var_samp(Quantity), stddev_pop(Quantity), stddev_samp(Quantity) FROM dfTable

# skewness and kurtosis
# Skewness and kurtosis are both measurements of extreme points in your data.
# Skewness measures the asymmetry of the values in your data around the mean
# kurtosis is a measure of the tail of data.
# These are both relevant specifically when modeling your data as a probability distribution of a random variable.
df.select(skewness("Quantity"), kurtosis("Quantity")).show()
# SELECT skewness(Quantity), kurtosis(Quantity) FROM dfTable

# Covariance and Correlation
# cov and corr functions compare the interactions of the values in two difference columns together.
# Correlation measures the Pearson correlation coefficient, which is scaled between –1 and +1.
# The covariance is scaled according to the inputs in the data.

df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"), covar_pop("InvoiceNo", "Quantity")).show()

# Aggregating to Complex Types
df.agg(collect_set("Country"), collect_list("Country")).show()

# Grouping
# perform calculations based on groups in the data
# group data on one column and perform calculations on the other columns in that group
# We do this grouping in two phases. First we specify the column(s) on which we would like to
# group, and then we specify the aggregation(s). The first step returns a
# RelationalGroupedDataset, and the second step returns a DataFrame.

df.groupBy("InvoiceNo", "CustomerId").count().show()
# SELECT count(*) FROM dfTable GROUP BY InvoiceNo, CustomerId

# Grouping with Expressions
df.groupBy("invoiceno").agg(count("quantity")).show()
df.groupBy("invoiceno").agg(expr("count(quantity)")).show()
df.groupBy("InvoiceNo").agg(count("Quantity").alias("quan"), expr("count(Quantity)")).show()

# Grouping with Maps
# specify your transformations as a series of Maps
# the key is the column, and the value is the aggregation function (as a string)
df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)")).show()
df.groupBy("InvoiceNo").agg(expr("avg(Quantity)").alias("avg_quan"),expr("stddev_pop(Quantity)").alias("stddev_quan")).show()

# Window Functions
# Spark supports three kinds of window functions: ranking functions, analytic functions, and aggregate functions
# A group-by takes data, and every row can go only into one grouping
# aggregation on a specific “window” of data, which is defined by reference to the current data.

"""
Caused by: org.apache.spark.SparkUpgradeException: 
You may get a different result due to the upgrading of Spark 3.0: 
Fail to parse '6/12/2011 13:17' in the new parser. 
You can set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0, 
or set to CORRECTED and treat it as an invalid datetime string.
"""

dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")
windowSpec = Window.partitionBy("CustomerId", "date").orderBy(desc("Quantity")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)
dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()

# Grouping Sets
# Grouping sets are a low-level tool for combining sets of aggregations together
# filter-out null values, to avoid incorrect results in cubes, rollups, and grouping sets.
dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

# The GROUPING SETS operator is only available in SQL. To perform the same in DataFrames, you
# use the rollup and cube operators—which allow us to get the same results.

# SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
# GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
# ORDER BY CustomerId DESC, stockCode DESC

# Rollups
# rollup is a multidimensional aggregation that performs a variety of group-by style calculations

rolledUpDF = dfNoNull.rollup("Date", "Country")\
    .agg(sum("Quantity"))\
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")\
    .orderBy("Date")
rolledUpDF.show()
rolledUpDF.where("Country IS NULL").show()
rolledUpDF.where("Date IS NULL").show()

"""
Caused by: org.apache.spark.SparkUpgradeException: 
You may get a different result due to the upgrading of Spark 3.0: 
Fail to parse '6/12/2011 13:17' in the new parser. 
You can set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0, 
or set to CORRECTED and treat it as an invalid datetime string.
"""

# Cube
# A cube takes the rollup to a level deeper. Rather than treating elements hierarchically, a cube
# does the same thing across all dimensions

dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))\
  .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()

"""
Caused by: org.apache.spark.SparkUpgradeException: 
You may get a different result due to the upgrading of Spark 3.0: 
Fail to parse '6/12/2011 13:17' in the new parser. 
You can set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0, 
or set to CORRECTED and treat it as an invalid datetime string.
"""

# Pivot
pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`").show()

# pyspark.sql.utils.AnalysisException: cannot resolve '`USA_sum(Quantity)`' given input columns:

