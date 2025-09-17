# Databricks notebook source
import datetime
from pyspark.sql import Row


user_list = [
    {'id': 1,'firstname': 'ravi','lastname': 'krishna', 'phone': Row(mobile=1234567890, home=9876543210),'course':[3],'email': 'ravi@gmail.com','amount': 1000.00,'primeUser': True,'country':'india','gender':'male', 'lasttxn': datetime.date(2020, 1, 1)},
    {'id': 2, 'firstname': 'giri','lastname': 'rao', 'phone': Row(mobile=8789098765, home=9876543210),'course':[1,3], 'email': 'giri@gmail.com', 'amount': 2000.00, 'primeUser': True,'country':'USA','gender':'male', 'lasttxn': datetime.date(2025, 1, 1)},
    {'id': 2, 'firstname': 'giri','lastname': 'rao', 'phone': Row(mobile=8789098765, home=9876543210),'course':[1,3], 'email': 'giri@gmail.com', 'amount': 3000.00, 'primeUser': True,'country':'USA','gender':'male', 'lasttxn': datetime.date(2025, 1, 1)},
    {'id': 3, 'firstname': 'hari','lastname': 'kanna', 'phone': None, 'course': None, 'email': 'hari@gmail.com', 'amount': 3000.00, 'primeUser': True,'country':'USA','gender':'female', 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 4, 'firstname': 'bari','lastname': 'munna', 'phone': Row(mobile=1234567890, home=9876543210),'course':[2,4], 'email': 'hari@gmail.com','amount':None , 'primeUser': False,'country':'india', 'gender':'male','lasttxn': datetime.date(2023, 1, 1)},
     {'id': 4, 'firstname': 'bari','lastname': 'munna', 'phone': Row(mobile=1234567890, home=9876543210),'course':[2,4], 'email': 'hari@gmail.com','amount':None , 'primeUser': False,'country':'india', 'gender':'male','lasttxn': datetime.date(2023, 1, 1)},   
    {'id': 5, 'firstname': 'pari','lastname': 'mama', 'phone': Row(mobile=9876543210, home=6578943210),'course':[1], 'email': 'hari@gmail.com', 'primeUser': False,'country':'india','gender':'female', 'lasttxn': datetime.date(2022, 1, 1)},
    {'id': None,'firstname': None,'lastname': None, 'phone': None,'course':[3],'email': None,'amount': None,'primeUser': None,'country':None,'gender':None, 'lasttxn': None},
    {'id': None,'firstname': 'chimpu','lastname':  'chimpu', 'phone': None,'course':[3],'email': None,'amount': 1000.00,'primeUser': None,'country':None,'gender':None, 'lasttxn':  datetime.date(2025, 5, 1)},    
    {'id': 6,'firstname': 'srinu','lastname': 'meenu', 'phone': None,'course':[3],'email': None,'amount': None,'primeUser': True,'country':'USA','gender':None, 'lasttxn': datetime.date(2025, 5, 1)},
    {'id': 7, 'firstname': 'raju','lastname': 'kaju', 'phone': Row(mobile=9876543210, home=6578943210),'course':None, 'email': 'hari@gmail.com', 'primeUser': None,'country':'india','gender':None, 'lasttxn': datetime.date(2022, 1, 1)},
    {'id': 8,'firstname': None,'lastname': None, 'phone': None,'course':[3],'email': None,'amount': None,'primeUser': True,'country':None,'gender':None, 'lasttxn': datetime.date(2025, 12, 1)}
   ] 

# COMMAND ----------

import pandas as pd

users = spark.createDataFrame(pd.DataFrame(user_list).astype(str))
users.printSchema()
users.show()

# COMMAND ----------

users.count()

# COMMAND ----------

users.distinct().count()

# COMMAND ----------

users.dropDuplicates().show()

# COMMAND ----------

users.dropDuplicates(['id','amount']).show()

# COMMAND ----------

users.dropDuplicates(['id']).show()

# COMMAND ----------

help(users.dropDuplicates)
