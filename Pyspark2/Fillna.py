# Databricks notebook source
emp = [{'id': 1, 'name': 'Alice', 'salary': 70000, 'bonus': 5000, 'country': 'USA', 'phone': '1234567890'}, 
       {'id': 2, 'name': 'Bob', 'salary': 80000, 'bonus': 6000, 'country': 'UK', 'phone': '0987654321'},
       {'id': 3, 'name': 'Charlie', 'salary': 90000, 'bonus': None, 'country': None, 'phone': None},
       {'id': 4, 'name': None, 'salary': 60000, 'bonus':'' , 'country': 'INDIA', 'phone': '74512498'},
       {'id': 5, 'name': 'Hary', 'salary': None, 'bonus':'' , 'country': None, 'phone': '74512498'}
       ]

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Row
schema=['id', 'name', 'salary', 'bonus', 'country', 'phone']
rows= [Row(**i) for i in emp]
df = spark.createDataFrame(rows,schema=schema)
df.show()



# COMMAND ----------

help(df.fillna)

# COMMAND ----------

df.show()

# COMMAND ----------

df.fillna(0).show()
df.fillna(0,'salary').show()
df.fillna(0,subset=['salary','bonus']).show()
df.fillna({'salary':-1 , 'bonus':0}).show()
df.fillna("Invalid",'country').show()

# COMMAND ----------

df.withColumn('bonus1', when(col('bonus')=='',0).otherwise(col('bonus'))).show()
