-- Databricks notebook source
select *from uc_merchandise_lakehouse.silver.orders_fact
where order_id='ON300101'

-- COMMAND ----------

select *from uc_merchandise_lakehouse.silver.customers_dim
where customer_key='9000000393'

-- COMMAND ----------

select *from uc_merchandise_lakehouse.silver.customers_dim
where customer_key='9000000393'

-- COMMAND ----------

select * from uc_merchandise_lakehouse.silver.orders_fact
where product_sk='e1f78785f4862a377497f4540a5d7bc8c5db16f5ad0f0eb03b798a8094c4aa36'


-- COMMAND ----------

select * from uc_merchandise_lakehouse.silver.products_dim
where sku_id='SKU1200'

-- COMMAND ----------

select * from uc_merchandise_lakehouse.bronze.offline_products
where sku_id='SKU1200'
