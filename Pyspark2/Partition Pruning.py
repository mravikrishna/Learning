# Databricks notebook source
import datetime
from pyspark.sql import Row

course_list= [
{"course_id":1,"course_title":"Java","course_pub_dt":datetime.date(2024,1,5),"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"course_id":2,"course_title":"Dotnet","course_pub_dt":datetime.date(2024,2,5),"isActive":True,"last_updt_time":datetime.datetime(2025,2,5,2,25,26)},
{"course_id":3,"course_title":"SAP","course_pub_dt":datetime.date(2025,2,5),"isActive":True,"last_updt_time":datetime.datetime(2025,3,5,2,25,26)},
{"course_id":4,"course_title":"Testing","course_pub_dt":datetime.date(2025,4,5),"isActive":False,"last_updt_time":datetime.datetime(2025,4,5,2,25,26)},
{"course_id":5,"course_title":"Python","course_pub_dt":datetime.date(2026,4,5),"isActive":True,"last_updt_time":datetime.datetime(2025,5,5,2,25,26)},
{"course_id":6,"course_title":"SQL","course_pub_dt":datetime.date(2026,5,5),"isActive":False,"last_updt_time":datetime.datetime(2025,6,5,2,25,26)},
]

# COMMAND ----------

df =spark.createDataFrame(course_list)
df.show()

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

df.dtypes

# COMMAND ----------

dbutils.fs.rm(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/",recurse=True)

# COMMAND ----------

from pyspark.sql.functions import *
df.withColumn('year',date_format(col('course_pub_dt'),'yyyy')).\
    coalesce(1).write.partitionBy('year').mode('overwrite').csv(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/")


# COMMAND ----------

dbutils.fs.ls(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/")

# COMMAND ----------

spark.read.csv(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/").filter('year=2024').show()

# COMMAND ----------

spark.read.csv(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/year=2024").show()

# COMMAND ----------

spark.read.csv(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/").filter('year=2024').count()

# COMMAND ----------

spark.read.csv(f"dbfs:/FileStore/{username}/erm_out/partition_by_date/year=2024").count()
