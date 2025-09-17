# Databricks notebook source
help(spark.udf.register)

# COMMAND ----------

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

df.dtypes

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
spark.udf.register("getCourse", lambda x: x , returnType=StringType())
df.selectExpr("getCourse(course_title)").show()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

spark.udf.register('active', lambda x: x, returnType=BooleanType())
df.selectExpr("active(isActive)").show()

# COMMAND ----------

import datetime
from pyspark.sql import Row

users = [
    {'user_id': 1,'firstname': 'ravi ','lastname': ' krishna', 'phone': Row(mobile=1234567890, home=9876543210),'email': 'ravi@gmail.com'},
    {'user_id': 2, 'firstname': 'giri ','lastname': 'rao ', 'phone': Row(mobile=8789098765, home=9876543210),'email': 'giri@gmail.com'},
    {'user_id': 3, 'firstname': ' hari','lastname': 'kanna ', 'phone': None,'email': 'hari@gmail.com'},
    {'user_id': 4, 'firstname': 'bari ','lastname': 'munna ', 'phone': Row(mobile=1234567890, home=9876543210),'email': 'bari@gmail.com'},
    {'user_id': 5, 'firstname': 'pari ','lastname': ' mama', 'phone': Row(mobile=9876543210, ), 'email': 'pari@gmail.com'}
  ]

# COMMAND ----------

import pandas as pd

userdf = spark.createDataFrame(pd.DataFrame(users))
userdf.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

userdf.select(trim(col('firstname'))).show()

# COMMAND ----------

def add(str):
    return str+" hello"

# COMMAND ----------

from pyspark.sql.functions import trim
spark.udf.register("con", lambda x: add(x.strip()) , returnType=StringType())
userdf.selectExpr("con(firstname)").show()
