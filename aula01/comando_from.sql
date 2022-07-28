-- Databricks notebook source
SELECT * 
FROM bronze_olist.olist_orders_dataset

-- COMMAND ----------

SELECT distinct order_status, count(0) as QTDE, 
FROM bronze_olist.olist_orders_dataset GROUP BY order_status;

-- COMMAND ----------


