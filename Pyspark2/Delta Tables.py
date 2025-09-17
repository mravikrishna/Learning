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

# MAGIC %sql show databases

# COMMAND ----------

# MAGIC %fs ls "/user/hive/warehouse/"
