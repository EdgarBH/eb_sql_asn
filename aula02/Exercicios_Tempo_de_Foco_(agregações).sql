-- Databricks notebook source
-- DBTITLE 1,1. Quantos vendedores são do estado de São Paulo?
SELECT descState,
      count(*) AS noVendedores 
FROM silver_olist.sellers 
WHERE descState='SP' 
GROUP BY descState

-- COMMAND ----------

-- DBTITLE 1,2. Quantos vendedores são de Presidente Prudente?
SELECT descCity,
      count(*) AS noVendedores 
FROM silver_olist.sellers 
WHERE descCity='presidente prudente' 
GROUP BY descCity

-- COMMAND ----------

-- DBTITLE 1,3. Quantos clientes são do estado do Rio de Janeiro?
SELECT descState,
      count(*) AS noVendedores 
FROM silver_olist.sellers 
WHERE descState='RJ' 
GROUP BY descState

-- COMMAND ----------

-- DBTITLE 1,4. Quantos produtos são de construção?
SELECT 
      CASE WHEN descCategoryName like '%const%' THEN 'construção'
      ELSE descCategoryName END AS newCategory,
      count(*) AS noProdutos 
FROM silver_olist.products 
WHERE descCategoryName like '%const%' 
GROUP BY newCategory

-- COMMAND ----------

-- DBTITLE 1,5. Qual o valor médio de um pedido?
SELECT mean(vlPrice) FROM silver_olist.order_items

-- COMMAND ----------

-- DBTITLE 1,6. Qual o valor médio do frete?
SELECT mean(vlFreight) FROM silver_olist.order_items

-- COMMAND ----------

-- DBTITLE 1,7. Em média os pedidos são de quantas parcelas de cartão?
SELECT mean(nrInstallments) FROM silver_olist.order_payment

-- COMMAND ----------

-- DBTITLE 1,8. Em média qual o valor médio por parcela?
SELECT mean(vlPayment/nrInstallments) FROM silver_olist.order_payment

-- COMMAND ----------

-- DBTITLE 1,9. Quanto tempo em média demora para um pedido chegar depois de aprovado?
SELECT avg(DATEDIFF(dtDeliveredCustomer,dtApproved)) AS avgDiasEntrega FROM silver_olist.orders WHERE DATEDIFF(dtDeliveredCustomer,dtApproved) is not Null 
