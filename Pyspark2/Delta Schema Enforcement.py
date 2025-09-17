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

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

delta_output_path= (f"dbfs:/FileStore/{username}/erm_out/delta/")
parquet_output_path= (f"dbfs:/FileStore/{username}/erm_out/parquet/")

# COMMAND ----------

dbutils.fs.rm(delta_output_path,recurse=True)
dbutils.fs.rm(parquet_output_path,recurse=True)

# COMMAND ----------

from pyspark.sql.functions import *

emp_df.coalesce(1).write.format('parquet').mode('overwrite').save(parquet_output_path)


# COMMAND ----------

dbutils.fs.ls(parquet_output_path)

# COMMAND ----------

display(spark.read.parquet(parquet_output_path))

# COMMAND ----------

emp_df.coalesce(1).write.format('delta').mode('overwrite').save(delta_output_path)


# COMMAND ----------

dbutils.fs.ls(delta_output_path)

# COMMAND ----------

display(spark.read.format('delta').load(delta_output_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/FileStore/spark-e0408fca-55e1-47ec-93bf-86/erm_out/delta/`

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/spark-e0408fca-55e1-47ec-93bf-86/erm_out/delta/_delta_log/'

# COMMAND ----------

display(spark.read.text('dbfs:/FileStore/spark-e0408fca-55e1-47ec-93bf-86/erm_out/delta/_delta_log/00000000000000000000.json'))

# COMMAND ----------

emp_df.dtypes

# COMMAND ----------

from pyspark.sql.types import IntegerType

def bonus_calc(sal,bonus):
  return sal + (sal * bonus)/100

# COMMAND ----------

from pyspark.sql.types import DoubleType
bonus_calc = spark.udf.register("bonus_calc", bonus_calc,DoubleType())

# COMMAND ----------

display(spark.read.parquet(parquet_output_path))

# COMMAND ----------

from pyspark.sql.functions import col, coalesce
add_col_parquet_df = spark.read.parquet(parquet_output_path)
add_col_parquet_df= add_col_parquet_df.withColumn("total_sal", bonus_calc(col("sal"), coalesce(col('bonus'),lit(0)).cast('int')))
display(add_col_parquet_df)

# COMMAND ----------

add_col_parquet_df.coalesce(1).write.format('parquet').mode('append').save(parquet_output_path)

# COMMAND ----------

dbutils.fs.ls(parquet_output_path)

# COMMAND ----------

display(spark.read.parquet(parquet_output_path))

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

add_col_delta_df = spark.read.format('delta').load(delta_output_path)
add_col_delta_df.show()

# COMMAND ----------

schema = StructType([
    StructField('empid',IntegerType()),
                StructField('name',StringType()),
    StructField('join_date',DateType()),
                StructField('mgrid',IntegerType()),
    StructField('sal',DoubleType()),
                StructField('bonus',IntegerType()),
    StructField('deptno',IntegerType()),
                StructField('isActive',IntegerType()),
    StructField('last_updt_time',DayTimeIntervalType())
])

# COMMAND ----------

add_col_delta_df.dtypes

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
#add_col_delta_df = add_col_delta_df.withColumn('bonus', col('bonus').cast('int')).fillna(0).withColumn('deptno', col('deptno').cast('int'))
add_col_delta_df = add_col_delta_df.withColumn("total_sal",bonus_calc(col('sal'),col('bonus').cast("int"))).withColumn('bonus',col('bonus').cast('string'))
display(add_col_delta_df)


# COMMAND ----------

dbutils.fs.ls(delta_output_path)

# COMMAND ----------

display(spark.read.text('dbfs:/FileStore/spark-e0408fca-55e1-47ec-93bf-86/erm_out/delta/_delta_log/00000000000000000000.json'))

# COMMAND ----------

spark.read.format('delta').load(delta_output_path).dtypes

# COMMAND ----------

add_col_delta_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Delta will not allow the schema change as we have added new column total salary 

# COMMAND ----------

add_col_delta_df.coalesce(1).write.format('delta').mode("append").save(delta_output_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Now using merge schema option we can merge the with new schema

# COMMAND ----------

add_col_delta_df.coalesce(1).write.format('delta').mode("append").option("mergeSchema","True").save(delta_output_path)

# COMMAND ----------

dbutils.fs.ls(delta_output_path)

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/spark-e0408fca-55e1-47ec-93bf-86/erm_out/delta/_delta_log/")

# COMMAND ----------

display(spark.read.text("dbfs:/FileStore/spark-e0408fca-55e1-47ec-93bf-86/erm_out/delta/_delta_log/00000000000000000001.json"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/FileStore/spark-e0408fca-55e1-47ec-93bf-86/erm_out/delta/`
