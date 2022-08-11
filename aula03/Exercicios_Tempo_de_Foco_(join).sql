-- Databricks notebook source
-- DBTITLE 1,1. Clientes de quais estados avaliam melhor?
SELECT 
--customers.idUniqueCustomer AS customer,
customers.descState AS estado,
order_review.vlScore AS score,
count(*) AS qtde
FROM silver_olist.customers AS customers
LEFT JOIN silver_olist.orders AS orders ON customers.idCustomer=orders.idCustomer 
LEFT JOIN silver_olist.order_review AS order_review ON orders.idOrder=order_review.idOrder
WHERE order_review.vlScore IS NOT NULL
GROUP BY customers.descState,order_review.vlScore ORDER BY score DESC , qtde DESC


-- COMMAND ----------

-- DBTITLE 1,2. Vendedores de quais estados têm as piores reputações?
SELECT 
DISTINCT sellers.descState AS estado,
order_review.vlScore AS score
FROM silver_olist.sellers AS sellers
LEFT JOIN silver_olist.order_items AS order_items ON sellers.idSeller=order_items.idSeller 
LEFT JOIN silver_olist.order_review AS order_review ON order_items.idOrder=order_review.idOrder
WHERE order_review.vlScore is not NULL AND order_review.vlScore=1



-- COMMAND ----------

-- DBTITLE 1,3. Quais estados levam mais tempo para a mercadoria chegar?
SELECT DATEDIFF(DATE(dtApproved),DATE(dtPurchase)) AS DATE_DIFF, * FROM silver_olist.orders AS orders LEFT JOIN silver_olist.customers AS customers ON orders.idCustomer=customers.idCustomer WHERE DATEDIFF(DATE(dtApproved),DATE(dtPurchase))>0 AND descStatus='delivered' ORDER BY  DATE_DIFF DESC

-- COMMAND ----------

-- DBTITLE 1,4. Quando o frete é mais caro que a mercadoria, os clientes preferem qual meio de pagamento?
SELECT
--   oitems.idOrder,
--   oitems.vlPrice,
--   oitems.vlFreight,
  opayment.descType, CASE
    WHEN oitems.vlPrice < oitems.vlFreight THEN 1
    ELSE 0 END AS vlFretemaior,
    count(oitems.idOrder) AS qtdPedidos,
    count(*) AS total
    FROM
      silver_olist.order_items AS oitems
      LEFT JOIN silver_olist.order_payment as opayment ON oitems.idOrder = opayment.idOrder
      GROUP BY vlFretemaior,opayment.descType
      ORDER BY qtdPedidos desc

-- COMMAND ----------

SELECT
  opayment.descType, 
    count(oitems.idOrder) AS qtdPedidos,
    ( SELECT count(*) FROM
      silver_olist.order_items AS oitems
      LEFT JOIN silver_olist.order_payment as opayment ON oitems.idOrder = opayment.idOrder
      WHERE oitems.vlPrice < oitems.vlFreight ) AS TOTAL,
    ROUND(count(oitems.idOrder)/( SELECT count(*) FROM
      silver_olist.order_items AS oitems
      LEFT JOIN silver_olist.order_payment as opayment ON oitems.idOrder = opayment.idOrder
      WHERE oitems.vlPrice < oitems.vlFreight ),4)*100 AS  PROPORCAO
    
    FROM
      silver_olist.order_items AS oitems
      LEFT JOIN silver_olist.order_payment as opayment ON oitems.idOrder = opayment.idOrder
      WHERE oitems.vlPrice < oitems.vlFreight
      GROUP BY opayment.descType
      ORDER BY qtdPedidos DESC



-- COMMAND ----------

-- DBTITLE 1,5. Qual meio de pagamento leva em média mais tempo para ser aprovado?


-- COMMAND ----------

-- DBTITLE 1,6. Qual categoria é campeã de vendas em cada estado?


-- COMMAND ----------

-- DBTITLE 1,7. Qual estado mais vende ferramenta?


-- COMMAND ----------

-- DBTITLE 1,8. Qual estado tem mais compras por cliente?

