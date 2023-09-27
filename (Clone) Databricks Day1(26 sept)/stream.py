# Databricks notebook source
dbutils.fs.mount(

  source = "wasbs://inputfiles@saunext.blob.core.windows.net",

  mount_point = "/mnt/saunext/inputfiles",

  extra_configs = {"fs.azure.account.key.saunext.blob.core.windows.net":"UUDMjjk8JYIiTwHNyh8WCs3BShkfIL//HM/cUrbOrRmUH+HaoR/J5bM9MlWTYefbkqNo/bQzgs1M+AStEn3dkA=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/saunext/inputfiles/inputstream

# COMMAND ----------

users_sch="timestamp timestamp, event_type string, user_id string, page_id string"

# COMMAND ----------

df=spark.readStream.schema(users_sch).json("dbfs:/mnt/saunext/inputfiles/inputstream/")

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.drop('page_id')

# COMMAND ----------

display(df)

# COMMAND ----------

outputstream="dbfs:/mnt/saunext/inputfiles/outputstream"

# COMMAND ----------



# COMMAND ----------

df.writeStream\
.option("checkpointlocation",f"{outputstream}/naval/checkpoint")\
.option("path",f"{outputstream}/ATUL/output")\
.table("test.jsonsample2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.jsonsample2

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

(spark

.readStream

.schema(users_sch)

.json("dbfs:/mnt/saunext/inputfiles/inputstream/")

.writeStream

.option("checkpointlocation",f"{outputstream}/naval/checkpoint")

.option("path",f"{outputstream}/naval/output")

.trigger(once=True)

.table("test.jsonsample")

)

# COMMAND ----------


