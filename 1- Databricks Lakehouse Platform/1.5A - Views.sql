-- Databricks notebook source
-- MAGIC %md
-- MAGIC * **Views** : Logical Query against source tables, Not Physical data
-- MAGIC 
-- MAGIC    * * -*Type of Views*: Stored(Views) - ,
-- MAGIC                       Temporary Views,
-- MAGIC                       Global Temporary views
-- MAGIC * **CTAS** 
-- MAGIC       ** Filtering and renaming Columns
-- MAGIC       
-- MAGIC       
-- MAGIC       
-- MAGIC       

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Views Comparison:**
-- MAGIC 
-- MAGIC | Views(Stored)    | Temp Views    | Global Temp Views |
-- MAGIC | -----------------| :-------------:| -----:            |
-- MAGIC | Persisted in DB  | session-Scoped | Cluster-Scoped    |
-- MAGIC | Dropped only by  DROP VIEW | Droppped when sessions ends      |   Dropped when Cluster Restarted |
-- MAGIC | created View    | Create Temp View   |  Create Global Temp View|
-- MAGIC 
-- MAGIC There must be at least 3 dashes separating each header cell.
-- MAGIC The outer pipes (|) are optional, and you don't need to make the 
-- MAGIC raw Markdown line up prettily. You can also use inline Markdown.
-- MAGIC 
-- MAGIC Markdown | Less | Pretty
-- MAGIC --- | --- | ---
-- MAGIC *Still* | `renders` | **nicely**
-- MAGIC 1 | 2 | 3

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- drop view view_apple_phones

-- COMMAND ----------

CREATE VIEW view_apple_phones
AS  SELECT * 
    FROM smartphones 
    WHERE brand = 'Apple';

-- COMMAND ----------

SELECT * FROM view_apple_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

CREATE TEMP VIEW temp_view_phones_brands
AS  SELECT DISTINCT brand
    FROM smartphones;

SELECT * FROM temp_view_phones_brands;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones
AS SELECT * FROM smartphones
    WHERE year > 2020
    ORDER BY year DESC;

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------


