-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Exercícios Bora praticar? 01

-- COMMAND ----------

-- DBTITLE 1,01.-Selecione todos os clientes paulistanos...
SELECT *
FROM bronze_olist.olist_customers_dataset WHERE customer_city='sao paulo'

-- COMMAND ----------

-- DBTITLE 1,02.-Selecione todos os clientes paulistas...
SELECT *
FROM bronze_olist.olist_customers_dataset WHERE customer_state='SP'

-- COMMAND ----------

-- DBTITLE 1,03.-Selecione todos vendedores cariocas e paulistas...
SELECT *
FROM bronze_olist.olist_sellers_dataset WHERE seller_city ='rio de janeiro' OR seller_state='SP'

-- COMMAND ----------

-- DBTITLE 1,04.-Selecione produtos de perfumaria e bebes com altura maior que 5cm
SELECT *
FROM bronze_olist.olist_products_dataset WHERE (product_category_name='perfumaria' OR product_category_name='bebes') AND product_height_cm > 5

-- COMMAND ----------

-- DBTITLE 1,04plus.-Selecione produtos de perfumaria e bebes e artes com altura maior que 5cm
SELECT *
FROM bronze_olist.olist_products_dataset WHERE product_category_name in ('perfumaria','bebes','artes') AND product_height_cm > 5
