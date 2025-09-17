# Databricks notebook source
# MAGIC %run "./DataFrame"

# COMMAND ----------

users.createOrReplaceTempView("users")

# COMMAND ----------

from pyspark.sql.functions import *


spark.sql("""
          select country,sum(amount) as total_revenue 
          from users
          group by  country
          """).show()

# COMMAND ----------

from pyspark.sql.functions import *
users.filter(users.country=='india').select(sum('amount').alias('total_revenue')).show()

# COMMAND ----------

from pyspark.sql.functions import col, sum 

users.groupBy('country').agg(sum('amount').alias('total_revenue'),count('id').alias('num_of_records')).show()


# COMMAND ----------

from pyspark.sql.functions import *

users.groupBy('primeUser').agg(sum('amount').alias('revenue')).show()

# COMMAND ----------

users.groupBy('country','primeUser').agg(sum('amount').alias('revenue')).sort('country',col('revenue').desc_nulls_last()).show()

# COMMAND ----------

users.groupBy('primeUser').agg(sum('amount').alias('total')).show()

# COMMAND ----------

users.select(sum('amount').alias('revenue'), count('id').alias('records')).show()

# COMMAND ----------

users.show()

# COMMAND ----------

users.groupBy().min().show()

# COMMAND ----------

users.groupBy().max().show()

# COMMAND ----------

users.groupBy('primeUser','country').agg(sum('amount').alias('revenue'),count('*').alias('records'),min('amount').alias('min_amt'),max('amount').alias('max_amt'),avg('amount').alias('avg_amt')).show()

# COMMAND ----------

prime_user =users.groupBy('primeUser')

# COMMAND ----------

prime_user.agg(sum('amount').alias('revenue')).show()

# COMMAND ----------

prime_user.agg({'amount':'sum','id':'count'}).show()

# COMMAND ----------

prime_user.agg({'amount':'sum','id':'count'}).toDF(*['primeUser','revenue','total_count']).show()
