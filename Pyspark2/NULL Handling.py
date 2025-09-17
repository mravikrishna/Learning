# Databricks notebook source
emp = [{'id': 1, 'name': 'Alice', 'salary': 70000, 'bonus': 5000, 'country': 'USA', 'phone': '1234567890'}, 
       {'id': 2, 'name': 'Bob', 'salary': 80000, 'bonus': 6000, 'country': 'UK', 'phone': '0987654321'},
       {'id': 3, 'name': 'Charlie', 'salary': 90000, 'bonus': None, 'country': 'Canada', 'phone': '111'},
       {'id': 4, 'name': 'Tom', 'salary': 60000, 'bonus':'' , 'country': 'INDIA', 'phone': '74512498'}
       ]

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Row
schema=['id', 'name', 'salary', 'bonus', 'country', 'phone']
rows= [Row(**i) for i in emp]
df = spark.createDataFrame(rows,schema=schema)
df.show()





# COMMAND ----------

from pyspark.sql.functions import coalesce, col

df = df.withColumn("bonus", coalesce(col('bonus'),lit('0')))
df.show()

# COMMAND ----------

df.createOrReplaceTempView("emp")
spark.sql("select (case when bonus='' then 0 else bonus end) as bonus from emp").show()


# COMMAND ----------

df.withColumn("bonus", when(col("bonus") == "", lit(0)).otherwise(col("bonus"))).show()
