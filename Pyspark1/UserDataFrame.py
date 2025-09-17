# Databricks notebook source
import datetime
from pyspark.sql import Row

users = [
    {'user_id': 1,'firstname': 'ravi','lastname': 'krishna', 'phone': Row(mobile=1234567890, home=9876543210),'email': 'ravi@gmail.com'},
    {'user_id': 2, 'firstname': 'giri','lastname': 'rao', 'phone': Row(mobile=8789098765, home=9876543210),'email': 'giri@gmail.com'},
    {'user_id': 3, 'firstname': 'hari','lastname': 'kanna', 'phone': None,'email': 'hari@gmail.com'},
    {'user_id': 4, 'firstname': 'bari','lastname': 'munna', 'phone': Row(mobile=1234567890, home=9876543210),'email': 'bari@gmail.com'},
    {'user_id': 5, 'firstname': 'pari','lastname': 'mama', 'phone': Row(mobile=9876543210, ), 'email': 'pari@gmail.com'}
  ]

# COMMAND ----------

import pandas as pd
from pyspark.sql import Row
#for i in users:
#    print(i)

userdf = spark.createDataFrame(pd.DataFrame(users))
userdf.show()

# COMMAND ----------

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

coursedf = spark.createDataFrame(pd.DataFrame(course_list))

coursedf.show()

# COMMAND ----------



course_enrolments=[
{"enrol_id":1,"user_id":1,"course_id":1,"fees_paid":100.00},
{"enrol_id":2,"user_id":2,"course_id":1,"fees_paid":500.00},
{"enrol_id":3,"user_id":5,"course_id":1,"fees_paid":750.00},
{"enrol_id":4,"user_id":4,"course_id":1,"fees_paid":600.00},
{"enrol_id":5,"user_id":5,"course_id":2,"fees_paid":200.00},
{"enrol_id":6,"user_id":1,"course_id":2,"fees_paid":100.00},
{"enrol_id":7,"user_id":1,"course_id":3,"fees_paid":500.00},
{"enrol_id":8,"user_id":1,"course_id":4,"fees_paid":100.00},
{"enrol_id":9,"user_id":2,"course_id":2,"fees_paid":200.00},
{"enrol_id":10,"user_id":2,"course_id":3,"fees_paid":200.00},
{"enrol_id":11,"user_id":4,"course_id":2,"fees_paid":600.00},
{"enrol_id":12,"user_id":5,"course_id":3,"fees_paid":500.00},
{"enrol_id":13,"user_id":4,"course_id":3,"fees_paid":400.00},
{"enrol_id":14,"user_id":2,"course_id":4,"fees_paid":300.00},
{"enrol_id":15,"user_id":2,"course_id":5,"fees_paid":200.00}
]

# COMMAND ----------

import pandas as pd

course_enrolmentsdf = spark.createDataFrame(pd.DataFrame(course_enrolments))
course_enrolmentsdf.show()
