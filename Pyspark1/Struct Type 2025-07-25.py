# Databricks notebook source
import datetime
from pyspark.sql import Row


user_list = [
    {'id': 1,'name': 'ravi', 'phone': Row(mobile=1234567890, home=9876543210),'email': 'ravi@gmail.com','amount': 1000.00,'primeUser': True, 'lasttxn': datetime.date(2020, 1, 1)},
    {'id': 2, 'name': 'giri', 'phone': Row(mobile=8789098765, home=9876543210), 'email': 'giri@gmail.com', 'amount': 2000.00, 'primeUser': True, 'lasttxn': datetime.date(2025, 1, 1)},
    {'id': 3, 'name': 'hari', 'phone': None, 'email': 'hari@gmail.com', 'amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 4, 'name': 'bari', 'phone': Row(mobile=1234567890, home=9876543210), 'email': 'hari@gmail.com','amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 5, 'name': 'pari', 'phone': Row(mobile=9876543210, home=6578943210), 'email': 'hari@gmail.com','amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
]

# COMMAND ----------

user_rows = [Row(**i) for i in user_list]

userdf= spark.createDataFrame(user_rows)
userdf.printSchema()
userdf.show(truncate=False)

# COMMAND ----------

userdf.select('id','phone.mobile','phone.home').show(truncate=False)



# COMMAND ----------

from pyspark.sql.functions import col,lit
userdf.select('id',col('phone')['mobile'],col('phone')['home']).show(truncate = False)

# COMMAND ----------

userdf.select(col('id'),col('phone.*')).show(truncate=False)
