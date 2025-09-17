# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/erm/order.csv

# COMMAND ----------

from pyspark.sql.functions import *

order_csvdf =spark.read.csv("dbfs:/FileStore/erm/order.csv",header=True,inferSchema=True)
display(order_csvdf)



# COMMAND ----------

from pyspark.sql.functions import *
# Convert the string to a timestamp
# The format pattern 'M/d/yyyy hh:mm:ss a' matches the input string
order_csvdf = order_csvdf.withColumn("order_date",to_timestamp(col("order_date"), "M/d/yyyy H:mm")).\
withColumn("order_month",col("order_date").cast("date")).\
withColumn("order_date_format",date_format('order_date','yyyy-MM-dd'))

# Show the result
order_csvdf.show()
order_csvdf.printSchema()



# COMMAND ----------

order_csvdf.filter(col('order_month') > to_date(lit("2025-05-04"), "yyyy-MM-dd")).show()

# COMMAND ----------

order_csvdf.filter(date_format('order_month','yyyyMM') == '202505').show()

# COMMAND ----------

order_csvdf.filter(date_format('order_month','yyyy-MM-dd')> lit('2025-05-06')).show()

# COMMAND ----------

from pyspark.sql.functions import lit, col

order_csvdf.filter(col('order_month') > '2025-05-02').show()

# COMMAND ----------

order_csvdf.groupBy('order_month').agg(count('*').alias('orders_count')).show()

# COMMAND ----------

order_csvdf.groupBy(date_format('order_month','yyyy-MM')).agg(count('*').alias('orders_count')).show()
