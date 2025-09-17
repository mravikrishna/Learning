# Databricks notebook source
# MAGIC %run "./Create Spark DataFrame"

# COMMAND ----------

userdf.filter(userdf.firstname=='ravi').show()


# COMMAND ----------

from pyspark.sql.functions import *

userdf.filter(userdf.amount.isNotNull()).show() 

# COMMAND ----------

userdf.filter(userdf.primeUser == True).show()

# COMMAND ----------

userdf.filter(col('primeUser')==True).show()

# COMMAND ----------

userdf.filter('primeUser == True').show()

# COMMAND ----------

userdf.filter('primeUser = "true"').show()

# COMMAND ----------

userdf.filter('primeUser=true').show()

# COMMAND ----------

userdf.filter(userdf.primeUser == True & (userdf.amount > 1000)).show()
              

# COMMAND ----------

userdf.createOrReplaceTempView("user")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from user

# COMMAND ----------

spark.sql(""" 
          select * from user
          """).show()

# COMMAND ----------

userdf.select("*").filter(col('lasttxn').between('2021-01-01','2025-01-01')).show()

# COMMAND ----------

userdf.select("*").filter("lasttxn between '2021-01-01' and '2025-01-01'").show()
