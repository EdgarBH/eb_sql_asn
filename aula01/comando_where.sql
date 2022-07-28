-- Databricks notebook source
SELECT *
FROM bronze_olist.olist_sellers_dataset
WHERE seller_city = 'rio de janeiro'

-- COMMAND ----------

SELECT seller_id
FROM bronze_olist.olist_sellers_dataset
WHERE seller_city = 'rio de janeiro'

-- COMMAND ----------

SELECT seller_id
FROM bronze_olist.olist_sellers_dataset
WHERE seller_city = 'rio de janeiro'

-- COMMAND ----------

SELECT * 
FROM bronze_olist.olist_orders_dataset
WHERE order_delivered_customer_date > order_estimated_delivery_date --Filtro

-- COMMAND ----------

SELECT *, 
date(order_delivered_customer_date) AS data_entrega,
date(order_estimated_delivery_date) AS data_prometida,
datediff(order_delivered_customer_date,order_estimated_delivery_date) AS dias_diferenca
FROM bronze_olist.olist_orders_dataset
WHERE date(order_delivered_customer_date) > date(order_estimated_delivery_date) --Filtro

-- COMMAND ----------


