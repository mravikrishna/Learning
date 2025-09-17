# Databricks notebook source
# MAGIC %run "./DataFrame"

# COMMAND ----------

users.show(truncate = False)

# COMMAND ----------

users.filter('amount is NULL').show()

# COMMAND ----------

from pyspark.sql.functions import col
users.filter(col('amount').isNull()).show()

# COMMAND ----------

users.drop('lasttxn').printSchema()

# COMMAND ----------

users.drop(col('lasttxn')).printSchema()

# COMMAND ----------

users.drop('id','firstname','lastname').printSchema()

# COMMAND ----------

drop_list = ['id','firstname','lastname']
users.drop(*drop_list).printSchema()
