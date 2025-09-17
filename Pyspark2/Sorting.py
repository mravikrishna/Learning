# Databricks notebook source
# MAGIC %run "./DataFrame"

# COMMAND ----------

users.sort('firstname').show()

# COMMAND ----------

users.orderBy('firstname').show()

# COMMAND ----------

users.sort(users.firstname).show()


# COMMAND ----------

users.sort(users['firstname']).show()

# COMMAND ----------

users.sort(users['amount']).show()

# COMMAND ----------

from pyspark.sql.functions import col,size
users.withColumn("size",size('course')).select(['id','course','size']).sort(col('size')).show()

# COMMAND ----------

users.sort(users.firstname,ascending=False).show()

# COMMAND ----------

c = users['firstname']

# COMMAND ----------

from pyspark.sql.functions import desc
users.sort(c.desc()).show()

# COMMAND ----------

users.sort(users.lasttxn.desc()).show()

# COMMAND ----------

users.select(['id','firstname','phone']).withColumn('no_of_phone', size('phone')).sort('no_of_phone', ascending=False).show()

# COMMAND ----------

users.orderBy('amount').show()

# COMMAND ----------

from pyspark.sql.functions import desc,col 
users.orderBy(col('amount').desc_nulls_last()).show()

# COMMAND ----------

users.orderBy(users['course'].desc_nulls_last()).show()

# COMMAND ----------

users.sort(users.course,users.amount).show()

# COMMAND ----------

from pyspark.sql.functions import col 
users.orderBy(col('course'), col('amount').desc_nulls_last()).show()

# COMMAND ----------

from pyspark.sql.functions import col,desc 

users.sort(col('course'), desc('amount')).show()

# COMMAND ----------

users.sort(['course','amount'],ascending=[0,1]).show()

# COMMAND ----------

from pyspark.sql.functions import when

users.createOrReplaceTempView("users")

spark.sql("select *,case when country == 'india' then 1 when country == 'USA' then 2 else 3 end as country_level from users order by country_level").show()

# COMMAND ----------

help(when)


# COMMAND ----------

from pyspark.sql.functions import col, when
country_level = (when(users.country == 'inida', 1).when(users.country == 'USA', 2).otherwise(3)).alias("country_level")
users.orderBy(country_level,col('amount').desc_nulls_last()).show()

# COMMAND ----------

from pyspark.sql.functions import expr

country_level = expr("case when country =='india' then 1 when country =='USA' then 2 else 3 end").alias("countr_level")

users.orderBy(country_level,col('amount').desc_nulls_last()).show()

