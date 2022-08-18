# cd D:\zikzakjack\code\git\Spark-The-Definitive-Guide
# pyspark --master local[*] --driver-memory 10G
# spark.sparkContext.setLogLevel("ERROR")

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("data/retail-data/by-day/2010-12-01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")

# Converting to Spark Types
df.select(lit(5), lit("five"), lit(5.0))
# SELECT 5, "five", 5.0

# Working with Booleans
df.where(col("InvoiceNo") != 536365).show(5, False)
df.where("InvoiceNo = 536365").show(5, False)
df.where("InvoiceNo <> 536365").show(5, False)

# chained filters
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()
df.where("StockCode in ('DOT')").where("UnitPrice > 600 OR instr(Description, 'POSTAGE') >= 1").show()
df.where(df.StockCode.isin("DOT")).where(df.UnitPrice > 600 OR instr(df.Description, "POSTAGE") >= 1).show()
# SELECT * FROM dfTable WHERE StockCode in ("DOT") AND(UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)

DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
  .where("isExpensive")\
  .select("unitPrice", "isExpensive").show(5)

# SELECT UnitPrice, (StockCode = 'DOT' AND (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
# FROM dfTable WHERE (StockCode = 'DOT' AND (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))

df.withColumn("isExpensive", expr("NOT UnitPrice <= 250")).where("isExpensive").select("Description", "UnitPrice").show(5)


# Working with Numbers
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)
df.selectExpr("CustomerId", "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)

# rounding
df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)


# correlation of two columns
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()
# SELECT corr(Quantity, UnitPrice) FROM dfTable

# compute summary statistics for a column or set of columns
# count / min / max / mean / std-dev
df.describe().show()

colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
df.stat.approxQuantile("UnitPrice", quantileProbs, relError) # 2.51
df.stat.crosstab("StockCode", "Quantity").show()
df.stat.freqItems(["StockCode", "Quantity"]).show()

# add a unique ID to each row
df.select(monotonically_increasing_id()).show(2)
df.withColumn("ids", monotonically_increasing_id()).show()

# Working with Strings
df.select(initcap(col("Description"))).show()
# SELECT initcap(Description) FROM dfTable

df.select(col("Description"), lower(col("Description")), upper(lower(col("Description")))).show(2)

df.select(
    ltrim(lit("    HELLO    ")).alias("ltrim"),
    rtrim(lit("    HELLO    ")).alias("rtrim"),
    trim(lit("    HELLO    ")).alias("trim"),
    lpad(lit("HELLO"), 3, " ").alias("lp"),
    rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)

regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
  regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
  col("Description")).show(2)

# replace given characters with other characters
df.select(translate(col("Description"), "LEET", "1337"),col("Description")).show(2)

extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
     regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
     col("Description")).show(2)

containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
  .where("hasSimpleColor")\
  .select("Description").show(3, False)

# SELECT Description FROM dfTable WHERE instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1

simpleColors = ["black", "white", "red", "green", "blue"]
def color_locator(column, color_string):
  return locate(color_string.upper(), column)\
          .cast("boolean")\
          .alias("is_" + color_string)
selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*")) # has to a be Column type

df.select(*selectedColumns).where(expr("is_white OR is_red"))\
  .select("Description").show(3, False)


# Working with Dates and Timestamps
dateDF = spark.range(10)\
  .withColumn("today", current_date())\
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")

dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
  .select(datediff(col("week_ago"), col("today"))).show(1)

dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))\
  .select(months_between(col("start"), col("end"))).show(1)

dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

spark.range(5).withColumn("date", lit("2017-01-01")).select(to_date(col("date"))).show(1)

dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")

cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
# SELECT cast(to_date("2017-01-01", "yyyy-dd-MM") as timestamp)

cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
cleanDateDF.filter(col("date2") > "'2017-12-12'").show()

# Working with Nulls in Data
# use nulls to represent missing or empty data in your DataFrames.

# Coalesce -> select the first non-null value from a set of columns
df.select(coalesce(col("Description"), col("CustomerId"))).show()

df.na.drop()
df.na.drop("any")
df.na.drop("all")
df.na.drop("all", subset=["StockCode", "InvoiceNo"])

# fill -> fill one or more columns with a set of values.
df.na.fill("all", subset=["StockCode", "InvoiceNo"])

fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}
df.na.fill(fill_cols_vals)

# replace
df.na.replace([""], ["UNKNOWN"], "Description")

# Ordering
asc_nulls_first
asc_nulls_last
desc_nulls_first
desc_nulls_last

# Working with Complex Types : structs, arrays, and maps.
complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")
complexDF.select("complex.Description")
complexDF.select(col("complex").getField("Description"))
complexDF.select("complex.*").show()

# Arrays
df.select(split(col("Description"), " ")).show(2)
df.select(split(col("Description"), " ").alias("array_col")).selectExpr("array_col[0]").show(2)
df.select(col("Description"), split(col("Description"), " ").alias("array_col"), size(split(col("Description"), " ")).alias("size")).show(truncate=False) # shows 5 and 3

df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

# explode -> takes a column that consists of arrays and creates one row (with the rest of the values duplicated) per value in the array

df.withColumn("splitted", split(col("Description"), " "))\
  .withColumn("exploded", explode(col("splitted")))\
  .select("Description", "InvoiceNo", "exploded").show(2)

# Maps
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2, truncate=False)

df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map")).selectExpr("complex_map['WHITE METAL LANTERN']").show(2, truncate=False)
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map")).selectExpr("explode(complex_map)").show(2, truncate=False)


# Working with JSON

jsonDF = spark.range(1).selectExpr("""
  '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")


# COMMAND ----------

from pyspark.sql.functions import get_json_object, json_tuple

jsonDF.select(get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column", json_tuple(col("jsonString"), "myJSONKey")).show(2)


df.selectExpr("(InvoiceNo, Description) as myStruct").select(to_json(col("myStruct"))).show(truncate=False)

parseSchema = StructType((
  StructField("InvoiceNo",StringType(),True),
  StructField("Description",StringType(),True)))
df.selectExpr("(InvoiceNo, Description) as myStruct")\
  .select(to_json(col("myStruct")).alias("newJSON"))\
  .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(truncate=False)

# User-Defined Functions
# UDFs can take and return one or more columns as input.
# UDF functions operate on the data, record by record.
# UDFs are registered as temporary functions to be used in that specific SparkSession or Context.

# If the function is
# written in Scala or Java, you can use it within the Java Virtual Machine (JVM). This means that
# there will be little performance penalty aside from the fact that you can’t take advantage of code
# generation capabilities that Spark has for built-in functions. There can be performance issues if
# you create or use a lot of objects;

# If the function is written in Python, something quite different happens. Spark starts a Python
# process on the worker, serializes all of the data to a format that Python can understand
# (remember, it was in the JVM earlier), executes the function row by row on that data in the
# Python process, and then finally returns the results of the row operations to the JVM and Spark.

udfExampleDF = spark.range(5).toDF("num")
def power3(double_value):
  return float(double_value ** 3)
power3(2.0)
power3udf = udf(power3)
udfExampleDF.select(power3udf(col("num"))).show()
spark.udf.register("power3py", power3, DoubleType())
udfExampleDF.selectExpr("power3py(num)").show()

