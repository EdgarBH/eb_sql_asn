-- Databricks notebook source
-- DBTITLE 1,Case
SELECT *,
CASE WHEN descState='SP' THEN 'paulista'
WHEN descState='RJ' THEN 'fluminense'
WHEN descState='MG' THEN 'mineiro' ELSE 'fodase' END AS descGentilico
FROM silver_olist.sellers

-- COMMAND ----------

with cte as(
SELECT *,
      CASE WHEN DATE(dtDeliveredCustomer)> DATE(dtEstimatedDelivered) THEN 'Entregue com atraso'
           WHEN DATE(dtDeliveredCustomer)= DATE(dtEstimatedDelivered) THEN 'Entregue em tempo'
           ELSE 'Chegou antes do previsto' END AS descAtraso,
           
      CASE WHEN DATE(dtDeliveredCustomer)> DATE(dtEstimatedDelivered) THEN 1
           WHEN DATE(dtDeliveredCustomer)= DATE(dtEstimatedDelivered) THEN 0
           ELSE 0 END AS flAtraso
      FROM silver_olist.orders
      )
SELECT mean(flAtraso) AS porAtraso from cte

-- COMMAND ----------

-- DBTITLE 1,Exercicio case
SELECT *,
      vlPrice+vlFreight AS valTotal,
      CAROUND(vlFreight/(vlPrice+vlFreight)*100,2) AS perEnv,
      CASE 
      WHEN (vlFreight/(vlPrice+vlFreight))*100 < 10 THEN '10%' 
      WHEN (vlFreight/(vlPrice+vlFreight))*100 < 25 THEN '10% a 25%'
      WHEN (vlFreight/(vlPrice+vlFreight))*100 < 50 THEN '25% a 50%'
      ELSE '+50%' END AS gpoEnv 
FROM silver_olist.order_items

-- COMMAND ----------


