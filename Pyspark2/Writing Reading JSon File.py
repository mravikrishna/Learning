# Databricks notebook source
import datetime
from pyspark.sql import Row

users = [
    {'user_id': 1,'firstname': 'ravi','lastname': 'krishna', 'phone': Row(mobile=1234567890, home=9876543210),'email': 'ravi@gmail.com'},
    {'user_id': 2, 'firstname': 'giri','lastname': 'rao', 'phone': Row(mobile=8789098765, home=9876543210),'email': 'giri@gmail.com'},
    {'user_id': 3, 'firstname': 'hari','lastname': 'kanna', 'phone': None,'email': 'hari@gmail.com'},
    {'user_id': 4, 'firstname': 'bari','lastname': 'munna', 'phone': Row(mobile=1234567890, home=9876543210),'email': 'bari@gmail.com'},
    {'user_id': 5, 'firstname': 'pari','lastname': 'mama', 'phone': Row(mobile=9876543210, ), 'email': 'pari@gmail.com'}
  ]

# COMMAND ----------

import pandas as pd
from pyspark.sql import Row
#for i in users:
#    print(i)

userdf = spark.createDataFrame(pd.DataFrame(users))
userdf.show()

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

userdf.coalesce(1).write.mode('overwrite').json(f"dbfs:/FileStore/{username}/erm_out/")

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/FileStore/{username}/erm_out/")

# COMMAND ----------

readdf = spark.read.json(f"dbfs:/FileStore/{username}/erm_out/")
readdf.show() 

# COMMAND ----------

userdf.coalesce(1).write.format('json').save(f"dbfs:/FileStore/{username}/erm_out/",mode='overwrite')

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/FileStore/{username}/erm_out/")

# COMMAND ----------

readdf = spark.read.json(f"dbfs:/FileStore/{username}/erm_out/")
readdf.show()

# COMMAND ----------

help(userdf.write.csv)

# COMMAND ----------

help(userdf.write.json)

# COMMAND ----------

help(userdf.write.parquet)
