-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Exercícios 01

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

-- COMMAND ----------

-- DBTITLE 1,05.-Lista de pedidos com mais de um item.
SELECT *
FROM bronze_olist.olist_order_items_dataset WHERE order_item_id >1

-- COMMAND ----------

-- DBTITLE 1,06.-Lista de pedidos que o frete é mais caro que o item.
SELECT * FROM bronze_olist.olist_order_items_dataset WHERE freight_value > price

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 07.-Lista de pedidos que ainda não foram enviados.
-- MAGIC Query usada: SELECT * FROM bronze_olist.olist_orders_dataset WHERE order_delivered=null

-- COMMAND ----------

SELECT * FROM bronze_olist.olist_orders_dataset WHERE order_delivered_customer_date is null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 08.-Lista de pedidos que foram entregues comatraso.
-- MAGIC Query usada: SELECT * FROM bronze_olist.olist_orders_dataset WHERE order_estimated_delivery_date < order_delivered_customer_date

-- COMMAND ----------

SELECT * FROM bronze_olist.olist_orders_dataset WHERE order_estimated_delivery_date < order_delivered_customer_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 09.-Lista de pedidos que foram entregues com2 dias de antecedência.
-- MAGIC Query usada: SELECT * FROM bronze_olist.olist_orders_dataset WHERE (order_delivered_customer_date - order_estimated_delivery_date)>=2

-- COMMAND ----------

SELECT order_delivered_customer_date - order_estimated_delivery_date AS diferenca,*  FROM bronze_olist.olist_orders_dataset 

-- COMMAND ----------

5. Lista de pedidos que foram entregues com2 dias de antecedência.6. Lista de pedidos feitos em dezembro de 2017 e entreguescomatraso7. Lista de pedidos com avaliação maior ou igual que 4
8. Lista de pedidos com 2 ou mais parcelas menores queR$20,00
