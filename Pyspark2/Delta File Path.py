# Databricks notebook source
import datetime
from pyspark.sql import Row

course_list= [
{"course_id":1,"course_title":"Java","course_pub_dt":datetime.date(2025,1,5),"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"course_id":2,"course_title":"Dotnet","course_pub_dt":datetime.date(2025,2,5),"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"course_id":3,"course_title":"SAP","course_pub_dt":datetime.date(2025,3,5),"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"course_id":4,"course_title":"Testing","course_pub_dt":datetime.date(2025,4,5),"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"course_id":5,"course_title":"Python","course_pub_dt":datetime.date(2025,5,5),"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"course_id":6,"course_title":"SQL","course_pub_dt":datetime.date(2025,6,5),"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
]

# COMMAND ----------

df =spark.createDataFrame(course_list)
df.show()

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

df.coalesce(1).write.mode('overwrite').parquet(f"dbfs:/FileStore/{username}/erm_out/parquet/")

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/FileStore/{username}/erm_out/parquet/")

# COMMAND ----------

#dbutils.fs.rm(f"dbfs:/FileStore/{username}/erm_out/parquet/",recurse=True)

# COMMAND ----------

df.coalesce(1).write.mode('overwrite').format('delta').save(f"dbfs:/FileStore/{username}/erm_out/delta/")

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/FileStore/{username}/erm_out/delta/")

# COMMAND ----------

# MAGIC %fs ls "dbfs:/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/_delta_log/"

# COMMAND ----------

spark.read.json("dbfs:/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/_delta_log/00000000000000000000.json").show(truncate = False)

# COMMAND ----------

display(spark.read.json("dbfs:/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/_delta_log/00000000000000000000.json"))

# COMMAND ----------

df.coalesce(1).write.mode('overwrite').format('delta').save(f"dbfs:/FileStore/{username}/erm_out/delta/")

# COMMAND ----------

spark.read.format("delta").load(f"dbfs:/FileStore/{username}/erm_out/delta/").show()

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/_delta_log/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  delta.`dbfs:/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/`

# COMMAND ----------

display(spark.read.text("dbfs:/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/_delta_log/00000000000000000001.json"))

# COMMAND ----------

import datetime
from pyspark.sql import Row

new_course_list= [
{"course_id":7,"course_title":"Kakfa","course_pub_dt":datetime.date(2026,1,5),"isActive":True,"last_updt_time":datetime.datetime(2026,1,5,2,25,26)},
{"course_id":8,"course_title":"Snowflake","course_pub_dt":datetime.date(2026,2,5),"isActive":True,"last_updt_time":datetime.datetime(2026,1,5,2,25,26)},
]

# COMMAND ----------

newdf =spark.createDataFrame(new_course_list)
newdf.show()

# COMMAND ----------

df =df.union(newdf)
df.show()

# COMMAND ----------

df.coalesce(1).write.format('delta').mode('overwrite').save(f"dbfs:/FileStore/{username}/erm_out/delta/")

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/FileStore/{username}/erm_out/delta/")

# COMMAND ----------

# MAGIC %fs ls "/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/"

# COMMAND ----------

# MAGIC %fs ls "/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/_delta_log/"

# COMMAND ----------

# MAGIC %sql select * from DELTA.`/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/`

# COMMAND ----------

display(spark.read.text("dbfs:/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/_delta_log/00000000000000000000.json"))

# COMMAND ----------

display(spark.read.text('dbfs:/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/_delta_log/00000000000000000001.json'))

# COMMAND ----------

display(spark.read.text("dbfs:/FileStore/spark-17e14d38-3339-4d1c-84a3-3f/erm_out/delta/_delta_log/00000000000000000002.json"))
