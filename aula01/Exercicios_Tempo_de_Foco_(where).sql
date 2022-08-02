-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Exercícios Bora praticar? 01

-- COMMAND ----------

-- DBTITLE 1,01.-Lista de pedidos com mais de um item.
SELECT *
FROM bronze_olist.olist_order_items_dataset WHERE order_item_id >1

-- COMMAND ----------

-- DBTITLE 1,02.-Lista de pedidos que o frete é mais caro que o item.
SELECT * FROM bronze_olist.olist_order_items_dataset WHERE freight_value > price

-- COMMAND ----------

-- DBTITLE 1,03.-Lista de pedidos que ainda não foram enviados.
SELECT * FROM bronze_olist.olist_orders_dataset WHERE order_delivered_customer_date is null

-- COMMAND ----------

-- DBTITLE 1,04.-Lista de pedidos que foram entregues comatraso.
SELECT * FROM bronze_olist.olist_orders_dataset WHERE order_estimated_delivery_date < order_delivered_customer_date

-- COMMAND ----------

-- DBTITLE 1,05.-Lista de pedidos que foram entregues com 2 dias de antecedência.
SELECT DATEDIFF(order_estimated_delivery_date,order_delivered_customer_date) AS DIAS_DE_ANTECEDENCIA, * FROM bronze_olist.olist_orders_dataset WHERE DATEDIFF(order_estimated_delivery_date,order_delivered_customer_date)>=2

-- COMMAND ----------

-- DBTITLE 1,06. Lista de pedidos feitos em dezembro de 2017 e entregues com atraso
SELECT DATEDIFF(order_delivered_customer_date,order_estimated_delivery_date) AS DIAS_DE_ATRASO, * FROM bronze_olist.olist_orders_dataset WHERE order_purchase_timestamp between '2017-12-01' AND '2017-12-31' AND DATEDIFF(order_delivered_customer_date,order_estimated_delivery_date)>1

-- COMMAND ----------

-- DBTITLE 1,07. Lista de pedidos com avaliação maior ou igual que 4
SELECT * FROM bronze_olist.olist_order_reviews_dataset WHERE review_score >= 4

-- COMMAND ----------

-- DBTITLE 1,08. Lista de pedidos com 2 ou mais parcelas menores que R$20,00
SELECT * FROM bronze_olist.olist_order_payments_dataset WHERE payment_installments>=2 AND payment_value<20

-- COMMAND ----------


