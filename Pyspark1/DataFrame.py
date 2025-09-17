# Databricks notebook source
import datetime
from pyspark.sql import Row


user_list = [
    {'id': 1,'firstname': 'ravi','lastname': 'krishna', 'phone': Row(mobile=1234567890, home=9876543210),'course':[3],'email': 'ravi@gmail.com','amount': 1000.00,'primeUser': True,'country':'India','gender':'male', 'lasttxn': datetime.date(2020, 1, 1)},
    {'id': 2, 'firstname': 'giri','lastname': 'rao', 'phone': Row(mobile=8789098765, home=9876543210),'course':[1,3], 'email': 'giri@gmail.com', 'amount': 2000.00, 'primeUser': True,'country':'USA','gender':'male', 'lasttxn': datetime.date(2025, 1, 1)},
    {'id': 3, 'firstname': 'hari','lastname': 'kanna', 'phone': None, 'course': None, 'email': 'hari@gmail.com', 'amount': 3000.00, 'primeUser': True,'country':'USA','gender':'female', 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 4, 'firstname': 'bari','lastname': 'munna', 'phone': Row(mobile=1234567890, home=9876543210),'course':[2,4], 'email': 'hari@gmail.com','amount':None , 'primeUser': False,'country':'India', 'gender':'male','lasttxn': datetime.date(2023, 1, 1)},
    {'id': 5, 'firstname': 'pari','lastname': 'mama', 'phone': Row(mobile=9876543210, home=6578943210),'course':[1], 'email': 'hari@gmail.com', 'amount':5000 ,'primeUser': True,'country':'India','gender':'female', 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 6, 'firstname': 'srinu','lastname': 'vasu', 'phone': Row(mobile=9876543210, home=6578943210),'course':[1,3], 'email': 'srinu@gmail.com', 'amount':None ,'primeUser': False,'country':'India','gender':'male', 'lasttxn': datetime.date(2023, 1, 1)},
    {'id': 7, 'firstname': 'shiva','lastname': 'dasu', 'phone': Row(mobile=9876543210, home=6578943210),'course':[2], 'email': 'shiva@gmail.com', 'amount':None ,'primeUser': False,'country':'UK','gender':'male', 'lasttxn': datetime.date(2023, 1, 1)},
    {'id': 8, 'firstname': 'bhrama','lastname': 'kamal', 'phone': Row(mobile=9876543210, home=6578943210),'course':[4], 'email': 'bhrama@gmail.com', 'amount':8000 ,'primeUser': True,'country':'UK','gender':'male', 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 9, 'firstname': 'vishnu','lastname': 'space', 'phone': Row(mobile=9876543210, home=6578943210),'course':[5], 'email': 'vishnu@gmail.com', 'amount':4000 ,'primeUser': True,'country':'UK','gender':'male', 'lasttxn': datetime.date(2024, 1, 1)},
]



# COMMAND ----------

import pandas as pd

users = spark.createDataFrame(pd.DataFrame(user_list))
users.show()
