# Databricks notebook source
# dbutils.fs.mount(

#   source = "wasbs://<containername>@<storageaccount>.blob.core.windows.net",

#   mount_point = "/mnt/input",

#   extra_configs = {"fs.azure.account.key.<storageaccount>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})`b

# COMMAND ----------

dbutils.fs.mount(

  source = "wasbs://raw@attuadls.blob.core.windows.net",

  mount_point = "/mnt/attuadls/raw",

  extra_configs = {"fs.azure.account.key.attuadls.blob.core.windows.net":"2ESi9ZwpLv/QY+7d8JDRePzti5mwGGgry7Pk2ZEXu+ocoEDTQd4j6LyDlz96oSFpG0cWr+Oyx1Zj+ASti0eI9g=="})

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/attuadls
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/attuadls/raw
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/attuadls/raw/json

# COMMAND ----------

output="dbfs:/mnt/attuadls/raw/json/14.8.2023.json"

# COMMAND ----------

df = spark.read \
  .option("inferSchema",True) \
  .option("header", True) \
  .json(f"{output}")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").json("dbfs:/mnt/attuadls/raw/output/json1")

# COMMAND ----------

# but there is problems because anyone can have access to keys thats why we use saas tockens 
# and from our side we need to unmount 
dbutils.fs.unmount('/mnt/sanly/raw')


# COMMAND ----------


