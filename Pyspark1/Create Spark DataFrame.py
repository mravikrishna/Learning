# Databricks notebook source
import datetime
from pyspark.sql import Row


user_list = [
    {'id': 1,'firstname': 'ravi','lastname': 'krishna', 'phone': Row(mobile=1234567890, home=9876543210),'course':[3],'email': 'ravi@gmail.com','amount': 1000.00,'primeUser': True, 'lasttxn': datetime.date(2020, 1, 1)},
    {'id': 2, 'firstname': 'giri','lastname': 'rao', 'phone': Row(mobile=8789098765, home=9876543210),'course':[1,3], 'email': 'giri@gmail.com', 'amount': 2000.00, 'primeUser': True, 'lasttxn': datetime.date(2025, 1, 1)},
    {'id': 3, 'firstname': 'hari','lastname': 'kanna', 'phone': None, 'course': None, 'email': 'hari@gmail.com', 'amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 4, 'firstname': 'bari','lastname': 'munna', 'phone': Row(mobile=1234567890, home=9876543210),'course':[2,4], 'email': 'hari@gmail.com','amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 5, 'firstname': 'pari','lastname': 'mama', 'phone': Row(mobile=9876543210, home=6578943210),'course':[1], 'email': 'hari@gmail.com','amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
]

# COMMAND ----------

import pandas as pd

#pd.DataFrame(user_list)
userdf = spark.createDataFrame(pd.DataFrame(user_list))
userdf.show(truncate=False)
