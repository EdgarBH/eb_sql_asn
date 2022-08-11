-- Databricks notebook source
-- DBTITLE 1,Qual categoria tem mais produtos vendidos?
SELECT 
  descCategoryName,
  count(*) as QTDE,
  count(DISTINCT idOrder, idOrderItem) AS QTDE_DISTINTA,
  count(*)-count(DISTINCT idOrder, idOrderItem) AS DIFERENCA
FROM silver_olist.order_items AS orderitems
LEFT JOIN silver_olist.products AS products ON orderitems.idProduct=products.idProduct
GROUP BY descCategoryName
ORDER BY QTDE DESC

-- COMMAND ----------

-- DBTITLE 1,Qual categoria tem produtos mais caros, em média?
SELECT
  descCategoryName,
  mean(vlPrice) AS precoMediomean,
  std(vlPrice) AS desvPad
FROM
  silver_olist.order_items AS orderitems 
  LEFT JOIN silver_olist.products AS products ON orderitems.idProduct = products.idProduct
GROUP BY
  descCategoryName
ORDER BY
  precoMediomean DESC

-- COMMAND ----------

-- DBTITLE 1,Qual categoria tem maiores fretes, em média?
SELECT
  descCategoryName,
  mean(vlFreight) AS freteMedio
FROM
  silver_olist.order_items AS orderitems 
  LEFT JOIN silver_olist.products AS products ON orderitems.idProduct = products.idProduct
GROUP BY
  descCategoryName
ORDER BY
  freteMedio DESC

-- COMMAND ----------

-- DBTITLE 1,Os clientes de qual estado pagam maior valor de frete, em media?
SELECT 
      --orderitems.idOrder,
      --orderitems.idOrderitem,
      AVG(orderitems.vlFreight) AS avgFrete,      
      --orders.idCustomer,
      customers.descState
FROM
  silver_olist.order_items AS orderitems
LEFT JOIN silver_olist.orders AS orders ON orderitems.idOrder=orders.idOrder
LEFT JOIN silver_olist.customers AS customers ON orders.idCustomer=customers.idCustomer
GROUP BY  customers.descState
ORDER BY avgFrete DESC
