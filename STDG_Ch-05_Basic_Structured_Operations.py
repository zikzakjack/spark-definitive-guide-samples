from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Zik-App").master("local[*]").enableHiveSupport().getOrCreate()

# A schema defines the column names and types of a DataFrame
spark.read.format("json").load("data/flight-data/json/2015-summary.json").schema

df = spark.read.format("json").load("data/flight-data/json/2015-summary.json")
df.printSchema()

# reating schema manually
myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])
df = spark.read.format("json").schema(myManualSchema).load("data/flight-data/json/2015-summary.json")

# col or column functions to construct and refer to columns
# Column and table resolution happens in the analyzer phase by comparing the catalog
col("someColumnName")
column("someColumnName")

# Explicit column references
df.col("count")

# Expressions
# An expression is a set of transformations on one or more values in a record in a DataFrame.
expr("(((someCol + 5) * 200) - 6) < otherCol")

# Accessing a DataFrame’s columns
df.columns

# Records and Rows

# return one or more Row types
df.first()

# Creating Rows
myRow = Row("Hello", None, 1, False)
myRow[0]
myRow[2]

# DataFrame Transformations
# We can add rows or columns
# We can remove rows or columns
# We can transform a row into a column (or vice versa)
# We can change the order of rows based on the values in columns

# Creating DataFrames from raw data sources
df = spark.read.format("json").load("data/flight-data/json/2015-summary.json")
df.createOrReplaceTempView("dfTable")

# create DataFrames on the fly by taking a set of rows
myManualSchema = StructType([
  StructField("some", StringType(), True),
  StructField("col", StringType(), True),
  StructField("names", LongType(), False)
])
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()


# select and selectExpr
# select and selectExpr allow you to do the DataFrame equivalent of SQL queries on a table of data:

df.select("DEST_COUNTRY_NAME").show(2)
# SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2


# select multiple columns by adding column name strings to select method call:
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)
# SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME FROM dfTable LIMIT 2

# refer to columns using expr() col() column()
# dont mix Column objects and strings

df.select(
    expr("DEST_COUNTRY_NAME"), \
    col("DEST_COUNTRY_NAME"), \
    column("DEST_COUNTRY_NAME"))\
  .show(2)

# expr() can refer to plain column or a string manipulation of a column
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
# SELECT DEST_COUNTRY_NAME as destination FROM dfTable LIMIT 2

# manipulate the result of your expression as another expression:
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show(2)


df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

# select followed by a series of expr = selectExpr
df.selectExpr(
  "*", # all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
  .show(2)
# SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry FROM dfTable LIMIT 2

# we can also specify aggregations
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
# SELECT avg(count), count(distinct(DEST_COUNTRY_NAME)) FROM dfTable LIMIT 2

# Converting to Spark Types (Literals)
# pass explicit values into Spark using literals.
df.select(expr("*"), lit(1).alias("One")).show(2)
df.selectExpr("*", " 1 as One").show(2)
# SELECT *, 1 as One FROM dfTable LIMIT 2

# Adding Columns
df.withColumn("numberOne", lit(1)).show(2)
# SELECT *, 1 as One FROM dfTable LIMIT 2

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)
df.selectExpr("*", "origin_country_name = dest_country_name as withinCountry").show(2)

df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).columns

# Rename a column
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").show(2)


# reserved characters like spaces or dashes in column names should be escaped appropriately using backtick
# We only need to escape expressions that use reserved characters or keywords.

dfWithLongColName = df.withColumn("This Long Column-Name", expr("ORIGIN_COUNTRY_NAME"))
dfWithLongColName.show(2)
dfWithLongColName.selectExpr("`This Long Column-Name`", "`This Long Column-Name` as `new col`").show(2)
dfWithLongColName.createOrReplaceTempView("dfTableLong")
# SELECT `This Long Column-Name`, `This Long Column-Name` as `new col` FROM dfTableLong LIMIT 2

dfWithLongColName.select(expr("`This Long Column-Name`")).columns
dfWithLongColName.select(expr("`This Long Column-Name`")).show(2)
dfWithLongColName.select("This Long Column-Name").show(2)

# Case Sensitivity
set spark.sql.caseSensitive true
spark.conf.set('spark.sql.caseSensitive', 'true')

# Removing Columns
df.drop("ORIGIN_COUNTRY_NAME").show(2)
df.drop("ORIGIN_COUNTRY_NAME").columns

dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").show(2)

# Changing a Column’s Type (cast)
df.printSchema()
df.withColumn("count2", col("count").cast("long"))
df.withColumn("count2", col("count").cast("string")).printSchema()

# SELECT *, cast(count as long) AS count2 FROM dfTable

# Filtering Rows
# filter rows using an expression that evaluates to true or false
# create either an expression as a String or build an expression by using a set of column manipulations
df.filter("count < 2").show(2)
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)
df.where(col("count") < 2).show(2)
# SELECT * FROM dfTable WHERE count < 2 LIMIT 2

# if you want to specify multiple AND filters, just chain them sequentially
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia").show(2)
df.where("count < 2").where(col("ORIGIN_COUNTRY_NAME") != "Croatia").show(2)
df.where("count < 2").where("ORIGIN_COUNTRY_NAME != 'Croatia'").show(2)
# SELECT * FROM dfTable WHERE count < 2 AND ORIGIN_COUNTRY_NAME != "Croatia" LIMIT 2

# Getting Unique Rows
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
# SELECT COUNT(DISTINCT(ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME)) FROM dfTable

df.select("ORIGIN_COUNTRY_NAME").distinct().count()
# SELECT COUNT(DISTINCT ORIGIN_COUNTRY_NAME) FROM dfTable

# Random Samples - sample some random records from your DataFrame
seed = 9840099767
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()

# Random Splits
# to break up your DataFrame into a random “splits”  to create training, validation, and test sets.
dataFrames = df.randomSplit([0.25, 0.75], seed)
dataFrames[0].count() > dataFrames[1].count() # False

# Concatenating and Appending Rows (Union)
# To union two DataFrames, you must be sure that they have the same schema and
# number of columns; otherwise, the union will fail
schema = df.schema
newRows = [
  Row("New Country", "Other Country", 5),
  Row("New Country 2", "Other Country 3", 1)
]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema)
df.union(newDF).where("count = 1").where(col("ORIGIN_COUNTRY_NAME") != "United States").show()

# Sorting Rows
df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

# An advanced tip is to use asc_nulls_first, desc_nulls_first, asc_nulls_last, or
# desc_nulls_last to specify where you would like your null values to appear in an ordered
# DataFrame.

# For optimization purposes, it’s sometimes advisable to sort within each partition before another
# set of transformations. You can use the sortWithinPartitions method to do this:
spark.read.format("json").load("data/flight-data/json/*-summary.json").sortWithinPartitions("count")


# Limit
df.limit(5).show()
# SELECT * FROM dfTable LIMIT 5

df.orderBy(expr("count desc")).limit(6).show()
# SELECT * FROM dfTable ORDER BY count desc LIMIT 6

# Repartition and Coalesce
# Another important optimization opportunity is to partition the data according to some frequently
# filtered columns, which control the physical layout of data across the cluster including the
# partitioning scheme and the number of partitions.
# Repartition will incur a full shuffle of the data, regardless of whether one is necessary. This
# means that you should typically only repartition when the future number of partitions is greater
# than your current number of partitions or when you are looking to partition by a set of columns:

df.rdd.getNumPartitions() # 1
df.repartition(5)
df.repartition(col("DEST_COUNTRY_NAME"))
df.repartition(5, col("DEST_COUNTRY_NAME"))
df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)


# Collecting Rows to the Driver
collectDF = df.limit(10)
collectDF.take(5) # take works with an Integer count
collectDF.show() # this prints it out nicely
collectDF.show(5, False)
collectDF.collect()
collectDF.toLocalIterator()