# Databricks notebook source
print("hello databricks ")

# COMMAND ----------

# magic command 
%sql
select "run sql "

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create database test

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC create table test.demo(id int, name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.demo

# COMMAND ----------

# MAGIC %sql
# MAGIC use test;
# MAGIC select * from test.demo
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use test;
# MAGIC
# MAGIC select * from emp_table

# COMMAND ----------


