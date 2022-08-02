-- Databricks notebook source
-- DBTITLE 1,01. Selecione todos os pedidos e marque se houve atraso ou não
SELECT 
CASE WHEN DATEDIFF(dtDeliveredCustomer,dtEstimatedDelivered)>0 THEN "PEDIDO_ATRASADO"
     WHEN DATEDIFF(dtDeliveredCustomer,dtEstimatedDelivered)<0 THEN "PEDIDO_ON_TIME" 
     ELSE "EM_TRANSITO" END AS STATUS_PEDIDO, 
COUNT(*) AS NO_PEDIDOS FROM silver_olist.orders  GROUP BY STATUS_PEDIDO

-- COMMAND ----------

-- DBTITLE 1,02. Selecione os pedidos e defina os grupos em uma novacoluna: ● para frete inferior à 10%: ‘10%’ ● para frete entre 10% e 25%: ‘10% a 25%’ ● para frete entre 25% e 50%: ‘25% a 50%’ ● para frete maior que 50%: ‘+50%’
-- 2. Selecione os pedidos e defina os grupos em uma novacoluna:
-- ● para frete inferior à 10%: ‘10%’
-- ● para frete entre 10% e 25%: ‘10% a 25%’
-- ● para frete entre 25% e 50%: ‘25% a 50%’
-- ● para frete maior que 50%: ‘+50%’

SELECT 
--       *,
--       vlPrice+vlFreight AS valTotal,
--       ROUND(vlFreight/(vlPrice+vlFreight)*100,2) AS perEnv,
      CASE 
      WHEN (vlFreight/(vlPrice+vlFreight))*100 < 10 THEN '10%' 
      WHEN (vlFreight/(vlPrice+vlFreight))*100 < 25 THEN '10% a 25%'
      WHEN (vlFreight/(vlPrice+vlFreight))*100 < 50 THEN '25% a 50%'
      ELSE '+50%' END AS gpoEnv,
      COUNT(*) AS qtde
FROM silver_olist.order_items GROUP BY gpoEnv ORDER BY qtde DESC

-- COMMAND ----------

-- DBTITLE 1,03.- Selecione a tabela olist.produto...
-- Selecione a tabela olist.produto :
-- ○ Crie uma coluna nova chamada ‘descNovaCategoria’ seguindo:i. agrupe ‘alimentos’ e ‘alimentos_bebidas’ em‘alimentos’
-- ii. agrupe ‘artes’ e ‘artes_e_artesanato’ em‘artes’
-- iii. agrupe todas categorias de construção emuma únicacategoriachamada ‘construção’
-- ○ Cria uma coluna nova chamada ‘descPeso’
-- i. para peso menor que 2kg: ‘leve’
-- ii. para peso entre 2kg e 5kg: ‘médio’
-- iii. para peso entre 5kg e 10kg: ‘pesado’
-- iv. para peso maior que 10kg: ‘muito pesado’

SELECT descCategoryName,
       CASE WHEN descCategoryName IN ('alimentos','alimentos_bebidas') THEN 'alimentos'
            WHEN descCategoryName IN ('artes','artes_e_artesanato') THEN 'artes' 
            WHEN descCategoryName LIKE '%const%' THEN 'construção' 
            ELSE descCategoryName END AS descNovaCategoria,
       CASE WHEN vlWeightGramas < 2000 THEN 'leve'
            WHEN vlWeightGramas < 5000 THEN 'médio'
            WHEN vlWeightGramas < 10000 THEN 'pesado'
            ELSE 'muito pesado' END AS descPeso,
            *
        FROM silver_olist.products

-- COMMAND ----------


