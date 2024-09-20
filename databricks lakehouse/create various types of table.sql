-- Databricks notebook source
-- MAGIC %md
-- MAGIC default database, default metastore(hive),managed table 

-- COMMAND ----------

create table employee(
  id INT,
  name varchar(30),
  dept VARCHAR(30),
  salary float
)

-- COMMAND ----------

INSERT INTO employee (id, name, dept, salary) VALUES 
(1, 'John Doe', 'HR', 12000),
(2, 'Jane Smith', 'IT', 15000),
(3, 'Robert Brown', 'Finance', 20000),
(4, 'Emily Davis', 'Marketing', 18000),
(5, 'Michael Johnson', 'HR', 25000),
(6, 'Linda White', 'IT', 30000),
(7, 'David Wilson', 'Finance', 17000),
(8, 'Jennifer Lee', 'Marketing', 13000),
(9, 'William Harris', 'HR', 29000),
(10, 'Patricia Clark', 'IT', 22000);

-- COMMAND ----------

describe detail employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC specified database ,default metastore , external table 

-- COMMAND ----------

CREATE TABLE  table_1 (
  id INT,
  name varchar(30),
  dept VARCHAR(30),
  salary float
)
USING DELTA
LOCATION ""
