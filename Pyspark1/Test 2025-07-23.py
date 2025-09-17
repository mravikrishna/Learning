# Databricks notebook source
def reverse(data):
    for index in range(len(data) - 1, -1, -1):
        print(data[index])


print(reverse("hello"))

# COMMAND ----------

site=spark.read.option("header","true").csv("dbfs:/FileStore/erm/likelihood_type.csv")
display(site)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/erm/
# MAGIC

# COMMAND ----------

for i in dbutils.fs.ls("/FileStore/erm"):
    print(i.path)
    

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

list = [1,2,3,4,5,6,7]

spark.createDataFrame(list,'int')


# COMMAND ----------

intlist = [1,2,3,4,5,6,7]

from pyspark.sql.types import IntegerType

spark.createDataFrame(intlist,IntegerType())


# COMMAND ----------

names =['ravi','hari','giri',1,2]

from pyspark.sql.types import StringType
df =spark.createDataFrame(names)

for i in df.collect():
    print(i)

# COMMAND ----------

age_list =[(1,),(2,),(3,)]
print(type(age_list[1]))
df =spark.createDataFrame(age_list, 'age int') 
for i in df.collect():
    print(i)

# COMMAND ----------

name_list =[(1,'ravi'),(2,'hari'),(3,'giri')]
for k,v in name_list:
    print(k,v)


# COMMAND ----------

name_list =[(1,'ravi'),(2,'hari'),(3,'giri')]

df =spark.createDataFrame(name_list,'no int,name string')

for i in df.collect():
    print(i.no,i.name)


