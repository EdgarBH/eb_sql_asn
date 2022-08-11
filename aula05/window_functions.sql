-- Databricks notebook source
select idOrder,
        idSeller,
        dtShippingLimit,
        ROW_NUMBER() OVER(PARTITION BY idSeller ORDER BY dtShippingLimit DESC) AS rownumber,
        LAG(dtShippingLimit) OVER(PARTITION BY idSeller ORDER BY dtShippingLimit DESC) AS lagShippingLimit,
        LEAD(dtShippingLimit) OVER(PARTITION BY idSeller ORDER BY dtShippingLimit DESC) AS ledShippingLimit
        FROM silver_olist.order_items

-- COMMAND ----------

WITH order_seller AS (
SELECT idSeller,
t2.dtApproved,
t1.idOrder,
SUM(vlPrice) AS totalPrice
FROM silver_olist.order_items AS t1
LEFT JOIN silver_olist.orders AS t2
ON t1.idOrder=t2.idOrder
GROUP BY idSeller,t1.idOrder,t2.dtApproved
),
rowNumber_seller AS (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY idSeller ORDER BY dtApproved DESC) AS rowNumber,
SUM(totalPrice) OVER(PARTITION BY idSeller ORDER BY dtApproved) AS sumAcum
FROM order_seller)
, 
result AS (
SELECT idSeller,
AVG(totalPrice) AS avgPrice
FROM rowNumber_seller WHERE rowNumber<=3
GROUP BY idSeller)

SELECT * FROM rowNumber_seller WHERE rowNumber<=3

-- COMMAND ----------

SELECT * FROM sandbox_apoiadores.tb_fodase

-- COMMAND ----------


