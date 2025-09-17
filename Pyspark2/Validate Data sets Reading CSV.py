# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/erm/

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/erm/order.csv',header=True,inferSchema=True)
df.printSchema()
df.show()

# COMMAND ----------

from pyspark.sql.functions import *

df.withColumn("to_order_date", to_date('order_date','M/d/yyyy H:mm')).show()


# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/erm/')

output_dir='dbfs:/FileStore/erm_out/'


# COMMAND ----------

for i in dbutils.fs.ls('dbfs:/FileStore/erm/'):
    df = spark.read.csv(i.path)
    print(i.name)
    df.write.parquet(mode='overwrite', path=output_dir)


# COMMAND ----------

dbutils.fs.ls(output_dir)

# COMMAND ----------

from pyspark.sql import Row 
from pyspark.sql.functions import *

parquet_df = spark.read.parquet(output_dir,header=True,inferSchema=True)
headers_df = parquet_df.head(1)

list =[]
for i in headers_df[0]:
    list.append(i)    

print(list)
datadf =parquet_df.filter(col('_c0') != headers_df[0]['_c0'])

newdf =datadf.toDF(*list)

newdf.printSchema()




# COMMAND ----------

from pyspark.sql import Row 
from pyspark.sql.functions import *

edf = spark.read.csv('dbfs:/FileStore/erm/employee.csv',header=True,inferSchema=True,sep="|")

#for i in edf.head(1)[0]:
#    print(i)


edf.show()



# COMMAND ----------

#import getpass
#username = getpass.getuser()
#print(username)
