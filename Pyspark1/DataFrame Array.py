# Databricks notebook source
import datetime
from pyspark.sql import Row 

customer_list = [
    {'id': 1, 'name': 'ravi', 'phone': 1234567890, 'email': 'ravi@gmail.com', 'amount': 1000, 'primeUser': True, 'lasttxn': datetime.date(2020, 1, 1)},
    {'id': 2, 'name': 'giri', 'phone': 1234567890, 'email': 'giri@gmail.com', 'amount': 2000, 'primeUser': True, 'lasttxn': datetime.date(2025, 1, 1)},
    {'id': 3, 'name': 'hari', 'phone': 1234567890, 'email': 'hari@gmail.com', 'amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 4, 'name': 'bari', 'phone': 1234567890, 'email': 'hari@gmail.com', 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 5, 'name': 'pari', 'phone': 1234567890, 'lasttxn': datetime.date(2024, 1, 1)},
]

customer_rows = [Row(**i) for i in customer_list]

headers = [i for i in customer_list[0].keys()]

#df = spark.createDataFrame(customer_rows,headers)
#df.show()

# COMMAND ----------

import pandas as pd

#pd.DataFrame(customer_rows)
pd.DataFrame(customer_list)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

user_schema = StructType([
    StructField('id',LongType()),
    StructField('name',StringType()),
    StructField('phone',IntegerType()),
    StructField('email',StringType()),
    StructField('amount',DoubleType()),
    StructField('primeUser',BooleanType()),
    StructField('lasttxn',DateType()),
])

df = spark.createDataFrame(pd.DataFrame(customer_list),schema=user_schema)
df.show()



# COMMAND ----------

users_list = [
    {'id': 1, 'name': 'ravi', 'phone': [1234567890,9876543210], 'email': 'ravi@gmail.com', 'amount': 1000.00, 'primeUser': True, 'lasttxn': datetime.date(2020, 1, 1)},
    {'id': 2, 'name': 'giri', 'phone': [8789098765,9876543210], 'email': 'giri@gmail.com', 'amount': 2000.00, 'primeUser': True, 'lasttxn': datetime.date(2025, 1, 1)},
    {'id': 3, 'name': 'hari', 'phone': None, 'email': 'hari@gmail.com', 'amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 4, 'name': 'bari', 'phone': [1234567890,9876543210,5432167890], 'email': 'hari@gmail.com','amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
    {'id': 5, 'name': 'pari', 'phone': [9876543210,6578943210], 'email': 'hari@gmail.com','amount': None, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
]


# COMMAND ----------

from pyspark.sql.functions import col,explode,explode_outer
from pyspark.sql import Row
import pandas as pd

#user_new_list = pd.DataFrame(users_list)
#df = spark.createDataFrame()
#df.show(truncate =False)

#user_rows = [Row(**i) for i in user_new_list.to_dict(orient='records')]

user_rows = [Row(**i) for i in users_list]

df = spark.createDataFrame(user_rows)
#df.printSchema()
#df.select('id','phone').show(truncate=False)
#df.withColumn('phone_numbers',explode_outer('phone')).drop('phone').show(truncate = False)

df.select(col('id'),col('phone')[0].alias('mobile'),col('phone')[1].alias('home')).show(truncate = False)

#df.columns
#df.dtypes



