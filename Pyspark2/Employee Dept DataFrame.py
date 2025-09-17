# Databricks notebook source
import datetime
from pyspark.sql import Row

emp_list= [
{"empid":1,"name":"Ravi","join_date":datetime.date(2025,1,5),"mgrid":2,"sal":5000.00,"bonus":2,"deptno":10,"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"empid":2,"name":"hari","join_date":datetime.date(2025,2,5),"mgrid":5,"sal":2000.00,"bonus":5,"deptno":10,"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"empid":3,"name":"giri","join_date":datetime.date(2025,3,5),"mgrid":4,"sal":6000.00,"bonus":None,"deptno":20,"isActive":False,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"empid":4,"name":"srinu","join_date":datetime.date(2025,4,5),"mgrid":6,"sal":9000.00,"bonus":7,"deptno":20,"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"empid":5,"name":"naresh","join_date":datetime.date(2025,5,5),"mgrid":6,"sal":15000.00,"bonus":10,"deptno":30,"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"empid":6,"name":"pari","join_date":datetime.date(2025,6,5),"mgrid":8,"sal":10000.00,"bonus":None,"deptno":30,"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"empid":7,"name":"lavi","join_date":datetime.date(2025,7,5),"mgrid":6,"sal":14000.00,"bonus":4,"deptno":40,"isActive":False,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)},
{"empid":8,"name":"kavi","join_date":datetime.date(2025,8,5),"mgrid":7,"sal":13000.00,"bonus":3,"deptno":None,"isActive":True,"last_updt_time":datetime.datetime(2025,1,5,2,25,26)}
]


# COMMAND ----------

import pandas as pd

emp_df =spark.createDataFrame(pd.DataFrame(emp_list))
emp_df.show()

# COMMAND ----------

import datetime
from pyspark.sql import Row

dept_list= [
{"deptno":10,"deptname":"IT","location":"Hyd"},
{"deptno":20,"deptname":"HR","location":"Bangalore"},
{"deptno":30,"deptname":"Sales","location":"Chennai"},
{"deptno":40,"deptname":"Finance","location":"Mumbai"},
{"deptno":50,"deptname":"Marketing","location":"Pune"}
]
dept_df =spark.createDataFrame(pd.DataFrame(dept_list))
dept_df.show()
