# Databricks notebook source
list =[('a')]

from pyspark.sql.functions import *

df = spark.createDataFrame(list,"name String")
df.printSchema()
df.show()


# COMMAND ----------

df.select(current_timestamp()).show(truncate = False)

# COMMAND ----------

from pyspark.sql.functions import *
df = spark.createDataFrame([1],['val'])
df.printSchema()

# COMMAND ----------

df = spark.createDataFrame([('2025-05-05',1)], ['dt', 'val'])
df.printSchema()

# COMMAND ----------

s="hello"

#help(range)

for i in range(len(s)-1,-1,-1):
    print(s[i])

# COMMAND ----------

from pyspark.sql.functions import substr
df = spark.createDataFrame([1234567890],['phone'])
df.select( substring(df.phone,6,5)).show()
df.select(substring(df.phone,-2,2)).show()

# COMMAND ----------

from pyspark.sql.functions import *
df.select(split(lit('hello, how are you')," ")).show()
df.select(split(lit('Hello, How are you')," ")[0]).show()
df.select(explode(split(lit('Hello, how are you')," "))).show() 

# COMMAND ----------

from pyspark.sql.functions import *
list = [{"name":"ravi", "phone":"123456,56789"},
 {"name":"hari", "phone":"123456,56789,789456"},
 {"name":"chipp", "phone":None}]

df = spark.createDataFrame(list,'name String,phone String') 
df.select (col('name'),split(col('phone'),",").alias("mobile")).show()

df.select(col('name'),explode(split(col('phone'),",")).alias('mobile')).show()

df.select(col('name'),explode_outer(split(col('phone'),",")).alias('mobile')).show()

# COMMAND ----------

df.groupBy('name').count().show()

# COMMAND ----------

df1=df.select(col('name'),explode_outer(split(col('phone'),",")).alias('mobile'))
df1.groupBy('name').agg(count('mobile')).show()

# COMMAND ----------

from pyspark.sql.functions import *
list = [{"name":"ravi", "phone":"123456,56789"},
 {"name":"hari", "phone":"123456,56789,789456"},
 {"name":"chipp", "phone":None}]

df = spark.createDataFrame(list,'name String,phone String') 
df.select(lpad(col('name'),10,'*')).show()





# COMMAND ----------

l =[('x')]

df = spark.createDataFrame(l,['name'])
df.select(current_timestamp(),current_date()).show(truncate = False)

df.select(to_date(lit('2025-05-05'),'yyyy-MM-dd')).show()


# COMMAND ----------

df.select(to_timestamp(lit('20250505'),'yyyyMMdd')).show()

# COMMAND ----------

from pyspark.sql.functions import *
df.select(date_add(current_date(),10),date_add(current_timestamp(),10),date_add(current_date(),-10)).show()

# COMMAND ----------

df.select(date_trunc('MM',current_date())).show()

# COMMAND ----------

#help(date_trunc)
df.select(date_trunc('day', current_date())).show()

# COMMAND ----------

df.select(to_date(lit('20250728'),'yyyyMMdd')).show()

# COMMAND ----------

df.select(to_date(lit('2025210'),'yyyyDDD')).show()

# COMMAND ----------


df.select(to_date(lit('28-October-2025'),'dd-MMMM-yyyy')).show()

# COMMAND ----------

df1=df.withColumn("new_date",(to_date(lit('2025-05-05'),'yyyy-MM-dd')))
df1.select(date_format(df1.new_date,'yyyyMM')).show()
