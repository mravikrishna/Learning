# Databricks notebook source
# MAGIC %run "./UserDataFrame"

# COMMAND ----------

course_enrolmentsdf.alias('ce').select('ce.*').show()

# COMMAND ----------

from pyspark.sql.functions import col 
course_enrolmentsdf.filter(col('user_id')==1).show()

# COMMAND ----------

userdf.join(course_enrolmentsdf, 'user_id').show(truncate= False)

# COMMAND ----------

userdf.join(course_enrolmentsdf,'user_id','left').show()

# COMMAND ----------

userdf.join(course_enrolmentsdf,'user_id','left_outer').show()

# COMMAND ----------

userdf.join(course_enrolmentsdf,'user_id','left_anti').show()

# COMMAND ----------

userdf.join(course_enrolmentsdf,'user_id','left').filter(course_enrolmentsdf.course_id.isNull()).show()

# COMMAND ----------

from pyspark.sql.functions import col 
userdf.join(course_enrolmentsdf,'user_id','left').filter(course_enrolmentsdf.course_id.isNull()).select(userdf['*']).show()

# COMMAND ----------

userdf.join(course_enrolmentsdf,'user_id','left').filter(course_enrolmentsdf.course_id.isNull()).select(userdf['*'],'enrol_id','course_id').show()

# COMMAND ----------

from pyspark.sql.functions import col
userdf.join(course_enrolmentsdf,'user_id','anti').show()

# COMMAND ----------

userdf.join(course_enrolmentsdf,'user_id','left_semi').show()

# COMMAND ----------

from pyspark.sql.functions import col, count, sum 
userdf.join(course_enrolmentsdf,'user_id','left').groupBy('user_id').agg(count('course_id').alias('course_enrolled')).show()


# COMMAND ----------

from pyspark.sql.functions import when, count, sum
userdf.join(course_enrolmentsdf,'user_id','left').groupBy('user_id').agg(sum(when(col('course_id').isNull(), 0).otherwise(1)).alias('course_enrolled')).show()

# COMMAND ----------

userdf.join(course_enrolmentsdf,'user_id','left').groupBy('user_id').agg(count('course_id').alias('course_enrolled')).orderBy('user_id').show()

# COMMAND ----------

userdf.join(course_enrolmentsdf,'user_id','full').orderBy('user_id').show()

# COMMAND ----------

userdf.join(course_enrolmentsdf,userdf.user_id==course_enrolmentsdf.user_id,'full').show()

# COMMAND ----------

coursedf.join(course_enrolmentsdf,'course_id','left_anti').show()

# COMMAND ----------

coursedf.join(course_enrolmentsdf,'course_id','left').filter('enrol_id is null').select(coursedf['*']).show()

# COMMAND ----------

userdf.show()
