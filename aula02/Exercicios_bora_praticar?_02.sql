-- Databricks notebook source
-- DBTITLE 1,Qual o pedido com maior valor de frete?
SELECT idOrder, vlFreight FROM silver_olist.order_items ORDER BY vlFreight DESC

-- COMMAND ----------

-- DBTITLE 1,Qual o pedido com menor valor de frete?
SELECT idOrder, vlFreight FROM silver_olist.order_items ORDER BY vlFreight ASC

-- COMMAND ----------

-- DBTITLE 1,Qual cliente tem mais pedidos?
SELECT idCustomer, count(*) as PEDIDOS FROM silver_olist.orders GROUP BY idCustomer ORDER BY PEDIDOS DESC

-- COMMAND ----------

-- DBTITLE 1,Qual vendedor tem mais itens vendidos?
 SELECT idSeller, count(*) AS ORDERS FROM silver_olist.order_items GROUP BY idSeller ORDER BY ORDERS DESC

-- COMMAND ----------

-- DBTITLE 1,Qual vendedor tem menos itens vendidos?
 SELECT idSeller, count(*) AS ORDERS FROM silver_olist.order_items GROUP BY idSeller ORDER BY ORDERS ASC

-- COMMAND ----------

-- DBTITLE 1,Qual dia tivemos mais pedidos?
SELECT DATE(dtPurchase) AS DATA_PEDIDO, COUNT(*) AS PEDIDOS FROM silver_olist.orders GROUP BY DATA_PEDIDO ORDER BY PEDIDOS DESC
