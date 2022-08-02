-- Databricks notebook source
-- DBTITLE 1,Qual categoria tem mais produtos vendidos?
SELECT idOrder, vlFreight FROM silver_olist.order_items ORDER BY vlFreight DESC

-- COMMAND ----------

-- DBTITLE 1,Qual categoria tem produtos mais caros, em média?
SELECT idOrder, vlFreight FROM silver_olist.order_items ORDER BY vlFreight ASC

-- COMMAND ----------

-- DBTITLE 1,Qual categoria tem maiores fretes, em média?
SELECT idCustomer, count(*) as PEDIDOS FROM silver_olist.orders GROUP BY idCustomer ORDER BY PEDIDOS DESC

-- COMMAND ----------

-- DBTITLE 1,Os clientes de qual estado pagam mais frete?
 SELECT idSeller, count(*) AS ORDERS FROM silver_olist.order_items GROUP BY idSeller ORDER BY ORDERS DESC

-- COMMAND ----------



-- COMMAND ----------


