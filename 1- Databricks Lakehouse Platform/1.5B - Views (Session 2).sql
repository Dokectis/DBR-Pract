-- Databricks notebook source
-- MAGIC %md
-- MAGIC **1.5B - Views (Session 2)**
-- MAGIC To validated if the Global Temp view from Previus .. Exist?

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

DROP TABLE smartphones;

DROP VIEW view_apple_phones;
DROP VIEW global_temp.global_temp_view_latest_phones;

-- COMMAND ----------


