# Databricks notebook source
import datetime
from pyspark.sql import Row

course_list= [
{"course_id":1,"course_title":"Java","course_pub_dt":datetime.date(2025,1,5),"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"course_id":2,"course_title":"Dotnet","course_pub_dt":datetime.date(2025,2,5),"isActive":True,"last_updt_time":datetime.datetime(2025,2,5,2,25,26)},
{"course_id":3,"course_title":"SAP","course_pub_dt":datetime.date(2025,2,5),"isActive":True,"last_updt_time":datetime.datetime(2025,3,5,2,25,26)},
{"course_id":4,"course_title":"Testing","course_pub_dt":datetime.date(2025,4,5),"isActive":False,"last_updt_time":datetime.datetime(2025,4,5,2,25,26)},
{"course_id":5,"course_title":"Python","course_pub_dt":datetime.date(2025,4,5),"isActive":True,"last_updt_time":datetime.datetime(2025,5,5,2,25,26)},
{"course_id":6,"course_title":"SQL","course_pub_dt":datetime.date(2025,5,5),"isActive":False,"last_updt_time":datetime.datetime(2025,6,5,2,25,26)},
]

# COMMAND ----------

df =spark.createDataFrame(course_list)
df.show()

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

df.coalesce(1).write.mode('overwrite').json(f"dbfs:/FileStore/{username}/erm_out/")

# COMMAND ----------

from pyspark.sql.functions import col 
readjsondf = spark.read.json(f"dbfs:/FileStore/{username}/erm_out/")
readjsondf.show()

readjsondf = readjsondf.withColumn('course_pub_dt', col('course_pub_dt').cast('date'))
readjsondf.dtypes

# COMMAND ----------

readjsondf = readjsondf.withColumn('last_updt_time',col('last_updt_time').cast('timestamp'))
readjsondf.dtypes
readjsondf.show()

# COMMAND ----------

from pyspark.sql.functions import date_format
readjsondf.withColumn("course_pub_dt",date_format('course_pub_dt','yyyyMM')).\
    coalesce(1).write.partitionBy("course_pub_dt").parquet(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/",mode='overwrite')
        

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/") 

# COMMAND ----------

spark.read.parquet(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/").dtypes

# COMMAND ----------

spark.read.parquet(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/").show()

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/FileStore/spark-e922b1d7-d5d1-4a11-b721-90/erm_out/partition_by_date/course_pub_dt=202501/")

# COMMAND ----------

from pyspark.sql.functions import date_format
readjsondf.withColumn("course_pub_dt",date_format('course_pub_dt','yyyyMM')).\
    coalesce(1).write.parquet(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/",mode='overwrite',partitionBy="course_pub_dt")
        

# COMMAND ----------

readjsondf.show()

# COMMAND ----------

df.show()

# COMMAND ----------

dbutils.fs.rm(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/",recurse=True)

# COMMAND ----------

df.withColumn("year",date_format(col("course_pub_dt"),"yyyy")).withColumn('month',date_format(col('course_pub_dt'),'MM')).\
    coalesce(1).write.parquet(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/",partitionBy=['year','month'],mode='overwrite')

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/")

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/year=2025/")

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/year=2025/month=02/")

# COMMAND ----------

spark.read.parquet(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/year=2025/")
