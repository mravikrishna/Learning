# Databricks notebook source
import datetime
user_list = [
    {'id': 1,'name': 'ravi', 'phone': {'mobile':1234567890,'home':9876543210},'email': 'ravi@gmail.com','amount': 1000.00,'primeUser': True, 'lasttxn': datetime.date(2020, 1, 1)},
    {'id': 2, 'name': 'giri', 'phone': {'mobile':8789098765,'home':9876543210}, 'email': 'giri@gmail.com', 'amount': 2000.00, 'primeUser': True, 'lasttxn': datetime.date(2025, 1, 1)},
    {'id': 3, 'name': 'hari', 'phone': None, 'email': 'hari@gmail.com', 'amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 4, 'name': 'bari', 'phone': {'mobile':1234567890,'home':9876543210}, 'email': 'hari@gmail.com','amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 5, 'name': 'pari', 'phone': {'mobile':9876543210,'home':6578943210}, 'email': 'hari@gmail.com','amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
]


# COMMAND ----------

from pyspark.sql import Row 
from pyspark.sql.types import *

user_rows= [Row(**i) for i in user_list]

df = spark.createDataFrame(user_rows)
#df.printSchema()
df.show(truncate =False)


# COMMAND ----------

from pyspark.sql.functions import col
df.select(col('id'),\
    col('phone')['mobile'].alias('mobile'),\
        col('phone')['home'].alias('home')).show(truncate = False) 

# COMMAND ----------

from pyspark.sql.functions import explode , explode_outer

df.select('id',explode('phone')).show(truncate=False)

df.select('id',explode_outer('phone')).show(truncate=False) 




# COMMAND ----------

from pyspark.sql.functions import explode, explode_outer
from pyspark.sql.functions import col

df.select('*',explode_outer('phone')).withColumnRenamed('key','phone_type').withColumnRenamed('value','phone_number') .drop('phone').show(truncate=False)
