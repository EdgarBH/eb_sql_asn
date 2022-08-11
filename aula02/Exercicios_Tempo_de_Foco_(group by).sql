-- Databricks notebook source
-- DBTITLE 1,1. Qual estado tem mais vendedores?


-- COMMAND ----------

-- DBTITLE 1,2. Qual cidade tem mais clientes?


-- COMMAND ----------

-- DBTITLE 1,3. Existe diferença entre os valores de pedido emrelaçãoaosmeiosdepagamento?


-- COMMAND ----------

-- DBTITLE 1,4. Qual categoria tem mais itens?


-- COMMAND ----------

-- DBTITLE 1,5. Qual categoria tem maior peso médio de produto?


-- COMMAND ----------

-- DBTITLE 1,6. Qual a série histórica de pedidos por dia?


-- COMMAND ----------

-- DBTITLE 1,7. Qual a série histórica de receita por dia?
SELECT
  DATE(dtApproved) AS dtAprovado,
  --COUNT(*), --Contar linhas não nulas
  COUNT(DISTINCT silver_olist.orders.idOrder) as qtPedidos,-- Contar a quantidade de pedidos diferentes
  SUM(vlPrice) AS vlDinheiro --Contar linhas não nulas
FROM
  silver_olist.orders
  LEFT JOIN silver_olist.order_items ON silver_olist.orders.idOrder = silver_olist.order_items.idOrder
  WHERE dtApproved IS NOT NULL
  GROUP BY DATE(dtApproved)
  ORDER BY DATE(dtApproved)


-- COMMAND ----------

SELECT COUNT(*) FROM silver_olist.orders

-- COMMAND ----------


