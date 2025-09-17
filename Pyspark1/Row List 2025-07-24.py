# Databricks notebook source
name_list =[(1,'ravi'),(2,'giri'),(3,'hari')]

df = spark.createDataFrame(name_list,['id','name'])

#df.show()
#display(df)

for i in df.collect():
    print(i)


# COMMAND ----------

from pyspark.sql.types import Row

help(Row)



# COMMAND ----------

x = Row(name='ravi',age=1)
print(x.name)
print(x['age'])

# COMMAND ----------

name_list=[[1,'ravi'],[2,'giri'],[3,'hari']]
df = spark.createDataFrame(name_list,['id','name'])
for i in df.collect():
    print(i)

# COMMAND ----------

users_list=[[1,'ravi'],[2,'giri'],[3,'hari']]

#df = spark.createDataFrame(users_list)

row_users = [Row(*i) for i in users_list]
#print(row_users)

df = spark.createDataFrame(row_users,['id','name'])

for d in df.collect():
    print(d)





# COMMAND ----------

def dummy(*args):
    print(args)

dummy(1)    

# COMMAND ----------

list = [1,'ravi']

dummy(*list)

# COMMAND ----------

name_list=[[1,'ravi'],[2,'giri'],[3,'hari']]
dummy(*name_list)

# COMMAND ----------

user_tuples = [(1,'ravi'),(2,'giri'),(3,'hari')]

user_rows = [Row(*i) for i in user_tuples]
#dummy(user_rows)

df = spark.createDataFrame(user_rows,['id','name'])



# COMMAND ----------

users_list =[

{'id':1,'name':'ravi'},
{'id':2,'name':'giri'},
{'id':3,'name':'hari'}

]

one_row = users_list[1]
print(one_row)
print(one_row.keys())
print(one_row.values())

users_rows = [Row(**i) for i in users_list]

print(users_rows)

#spark.createDataFrame(users_rows,['id', 'name'])

#for i in df.collect():
#    print(i)

  

# COMMAND ----------

def kw(**kwargs):
    print(kwargs)


users_list =[

{'id':1,'name':'ravi'},
{'id':2,'name':'giri'},
{'id':3,'name':'hari'}

]

userlist= users_list[0]

kw(user_list=users_list)    
kw(**userlist)


# COMMAND ----------

import datetime
from pyspark.sql import Row 

customer_list = [
    {'id': 1, 'name': 'ravi', 'phone': 1234567890, 'email': 'ravi@gmail.com', 'amount': 1000, 'primeUser': True, 'lasttxn': datetime.date(2020, 1, 1)},
    {'id': 2, 'name': 'giri', 'phone': 1234567890, 'email': 'giri@gmail.com', 'amount': 2000, 'primeUser': True, 'lasttxn': datetime.date(2025, 1, 1)},
    {'id': 3, 'name': 'hari', 'phone': 1234567890, 'email': 'hari@gmail.com', 'amount': 3000, 'primeUser': False, 'lasttxn': datetime.date(2024, 1, 1)},
]

customer_rows = [Row(**i) for i in customer_list]

headers = [i for i in customer_list[0].keys()]

#df = spark.createDataFrame(customer_rows,headers)
