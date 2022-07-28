-- Databricks notebook source
SELECT * FROM silver_olist.sellers

-- COMMAND ----------

-- DBTITLE 1,Número de linhas
SELECT count(*) FROM silver_olist.sellers

-- COMMAND ----------

-- DBTITLE 1,Distinct
SELECT COUNT(*) AS qtLinhas, --número de linhas
       COUNT(DISTINCT idSeller) AS qtSeller, -- Sellers distintos
       COUNT(DISTINCT descState) AS qtEstadosDistinct, --estados distintos
       COUNT(descState) AS qtEstadosDistinct --estados não nulos
FROM silver_olist.sellers

-- COMMAND ----------

-- DBTITLE 1,Counts
SELECT 
COUNT(*) AS qtlinhas, -- O asterisco retorna a quantidade de linhas
COUNT(DISTINCT idOrder) AS qtPedidos, -- O Distinct permite detectar inconsistencias
MEAN(DATEDIFF(DATE(dtDeliveredCustomer),DATE(dtEstimatedDelivered))) AS meanAtraso,
AVG(DATEDIFF(DATE(dtDeliveredCustomer),DATE(dtEstimatedDelivered))) AS avgAtraso,
STD(DATEDIFF(DATE(dtDeliveredCustomer),DATE(dtEstimatedDelivered))) AS stdAtraso,
AVG(DATEDIFF(DATE(dtDeliveredCustomer),DATE(dtEstimatedDelivered))) + 1.96 * STD(DATEDIFF(DATE(dtDeliveredCustomer),DATE(dtEstimatedDelivered))) AS nrLimiteSuperior,
AVG(DATEDIFF(DATE(dtDeliveredCustomer),DATE(dtEstimatedDelivered))) - 1.96 * STD(DATEDIFF(DATE(dtDeliveredCustomer),DATE(dtEstimatedDelivered))) AS nrLimiteInferior,
MIN(DATEDIFF(DATE(dtDeliveredCustomer),DATE(dtEstimatedDelivered))) AS minDiasAtraso,
MAX(DATEDIFF(DATE(dtDeliveredCustomer),DATE(dtEstimatedDelivered))) AS maxDiasAtraso
"""MAX(DATEDIFF(DATE(dtDeliveredCustomer),DATE(dtEstimatedDelivered))) AS maxDiasAtraso
FROM silver_olist.orders
WHERE DATE(dtDeliveredCustomer) > DATE(dtEstimatedDelivered)"""


-- COMMAND ----------

SELECT *,
DATEDIFF(DATE(dtDeliveredCustomer),DATE(dtEstimatedDelivered)) AS qtDiasAtraso
FROM silver_olist.orders
WHERE DATE(dtDeliveredCustomer) > DATE(dtEstimatedDelivered)

-- COMMAND ----------

SELECT COUNT(DISTINCT idProduct) AS qtProduto
FROM silver_olist.products
WHERE descCategoryName='bebes'

-- COMMAND ----------

-- DBTITLE 1,Quantidade de produtos por grupo
SELECT 
descCategoryName,
COUNT(DISTINCT idProduct) AS qtProduto,
AVG(vlWeightGramas) as avgPesogr,
MEAN(vlWeightGramas)/1000 as avgPesokg
FROM silver_olist.products
WHERE descCategoryName IS NOT NULL
GROUP BY descCategoryName ORDER BY qtProduto DESC

-- COMMAND ----------

SELECT 
      CASE WHEN DATE(dtDeliveredCustomer) > dtEstimatedDelivered THEN 'atraso'
      ELSE 'no atraso' END AS flAtraso,
      COUNT(idOrder) AS qtPedido
FROM silver_olist.order_ite

-- COMMAND ----------

-- DBTITLE 1,Quantidade de produtos com HAVING (Filtro pos agrupamento)
SELECT 
descCategoryName,
COUNT(DISTINCT idProduct) AS qtProduto,
AVG(vlWeightGramas) as avgPesogr,
MEAN(vlWeightGramas)/1000 as avgPesokg
FROM silver_olist.products
WHERE descCategoryName IS NOT NULL
GROUP BY descCategoryName 
HAVING COUNT(DISTINCT idProduct)>=50
ORDER BY qtProduto ASC, avgPesokg ASC  --O ORDER BY é a última coisa que se executa na Query

-- COMMAND ----------


