# Databricks notebook source
# MAGIC %run "./Create Spark DataFrame"

# COMMAND ----------

userdf.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import explode,explode_outer
userdf.select('*',explode_outer('phone').alias('phone_numbers')).show()

# COMMAND ----------

userdf.alias('u').select('u.*').show()

# COMMAND ----------

userdf.selectExpr('*').show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import *
userdf.select('id', concat(col('firstname'), lit(' '), col('lastname')).alias('fullname')).show(truncate=False)

# COMMAND ----------

userdf.select(['id','firstname','lastname']).show()


# COMMAND ----------

userdf.selectExpr('id','firstname','lastname',"concat(firstname,',',lastname) as full_name",'length(lastname) as len').show()

# COMMAND ----------

userdf.createOrReplaceTempView('user')

# COMMAND ----------

spark.sql("""
          select id,firstname,lastname,concat(firstname,',',lastname) as full_name from user
          
          """).show()

# COMMAND ----------

from pyspark.sql.functions import col, lit

userdf.select(userdf['id'],col('firstname'),'lastname').show()

# COMMAND ----------

spark.sql("""
          select u.id, u.firstname from user u
          
          """).show()

# COMMAND ----------

cols = ['id','firstname','lastname','phone']
userdf.select(*cols).show(truncate = False)


# COMMAND ----------

userdf.show()
