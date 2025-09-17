# Databricks notebook source
# MAGIC %run "./Create Spark DataFrame"

# COMMAND ----------

userdf.select('id','lasttxn').printSchema()

# COMMAND ----------

from pyspark.sql.functions import date_format
#userdf.withColumn('lasttxn', date_format('lasttxn', 'yyyyMMdd').cast('int'))
userdf.printSchema()


# COMMAND ----------

from pyspark.sql.functions import date_format
userdf.printSchema()
cols = ['id',date_format('lasttxn','ddmmyyyy').cast('int').alias('lasttxn')]

userdf.select(*cols).show()


# COMMAND ----------

from pyspark.sql.functions import col, lit ,concat

fullname = concat(col('firstname'), lit(','), col('lastname'))

userdf.select(col('id'), fullname.alias('fullname')).show()
             

# COMMAND ----------

old =['id','firstname','lastname','phone','course']
new =['user_id','first_name','last_name','phone_numbers','courses']

userdf.select(old).toDF(*new).show()


# COMMAND ----------

def convert(*cols):
    print(type(cols))
    print(cols)

convert(*['a','b'])    
