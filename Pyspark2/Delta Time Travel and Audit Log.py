# Databricks notebook source
# MAGIC %run "./Employee Dept DataFrame"

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

delta_output_path= (f"dbfs:/FileStore/{username}/erm_out/delta/")
parquet_output_path= (f"dbfs:/FileStore/{username}/erm_out/parquet/")

# COMMAND ----------

dbutils.fs.rm(delta_output_path)
dbutils.fs.rm(parquet_output_path)

# COMMAND ----------

emp_df.write.format("delta").mode('overwrite').save(delta_output_path)

# COMMAND ----------

dbutils.fs.ls(delta_output_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/`

# COMMAND ----------

spark.read.format('delta').load('dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/').dtypes

# COMMAND ----------

import datetime
from pyspark.sql import Row
import pandas as pd

new_emp_list= [
{"empid":9,"name":"kush","join_date":datetime.date(2026,7,5),"mgrid":8,"sal":12000.00,"bonus":4,"deptno":30,"isActive":False,"last_updt_time":datetime.datetime(2026,1,5,2,25,26)},
{"empid":10,"name":"lav","join_date":datetime.date(2026,8,5),"mgrid":8,"sal":13000.00,"bonus":6,"deptno":None,"isActive":True,"last_updt_time":datetime.datetime(2026,1,5,2,25,26)}
]

new_emp_df = spark.createDataFrame(pd.DataFrame(new_emp_list))
new_emp_df.dtypes

# COMMAND ----------

emp_df = emp_df.union(new_emp_df)
emp_df.show()

# COMMAND ----------

emp_df.write.format("delta").mode('append').save(delta_output_path)

# COMMAND ----------

spark.read.format('delta').load(delta_output_path).show()

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/_delta_log')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/`

# COMMAND ----------

# MAGIC %sql describe history delta.`dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/` version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/` version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/` version as of 0

# COMMAND ----------

# MAGIC %sql describe history delta.`dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/` 

# COMMAND ----------

# MAGIC %sql select * from delta.`dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/` timestamp as of '2025-07-30T08:06:45.000+00:00'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/_delta_log/'

# COMMAND ----------

# MAGIC %fs head 'dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/_delta_log/00000000000000000002.crc'

# COMMAND ----------

# MAGIC %fs head 'dbfs:/FileStore/spark-5c1b6976-799b-460e-8732-f9/erm_out/delta/_delta_log/00000000000000000002.json'
