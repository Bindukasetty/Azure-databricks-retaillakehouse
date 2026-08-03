-- Step 1: Offline sales — resolve raw ids to business keys -------------------
-- offline_orders references customer_id / product_id directly (not the
-- unified customer_key / sku_id), so both need a dimension lookup.
-- offline_orders only has a date (no time-of-day), so we treat it as midnight
-- for the point-in-time comparison.

CREATE OR REFRESH STREAMING TABLE stg_offline_sales AS
SELECT
  o.order_id,
  CAST(o.order_date AS TIMESTAMP)          AS order_ts,
  'offline'                                 AS channel,
  o.payment_mode,
  cd.customer_key,
  pd.product_sk,
  o.quantity,
  o.unit_price,
  o.quantity * o.unit_price                 AS total_amount,
  CAST(o.customer_id AS STRING)             AS _source_customer_ref,
  CAST(o.product_id AS STRING)              AS _source_product_ref
FROM STREAM(uc_merchandise_lakehouse.bronze.offline_orders) o
LEFT JOIN uc_merchandise_lakehouse.silver.customers_dim cd
  ON cd.offline_customer_id = o.customer_id
LEFT JOIN uc_merchandise_lakehouse.silver.products_dim pd
  ON pd.offline_product_id = o.product_id
 AND pd.__END_AT IS NULL;

-- Step 2: Online sales — explode items, resolve customer, point-in-time product

CREATE OR REFRESH STREAMING TABLE stg_online_sales AS
WITH exploded AS (
  SELECT
    s.order_id,
    s.order_timestamp,
    s.payment_mode,
    s.customer.customer_id AS online_customer_id,
    item.sku_id            AS item_sku_id,
    item.quantity          AS item_quantity,
    item.amount            AS item_amount
  FROM STREAM(uc_merchandise_lakehouse.bronze.online_sales_rawdata) s
  LATERAL VIEW explode(s.items) item_tbl AS item
)
SELECT
  e.order_id,
  CAST(e.order_timestamp AS TIMESTAMP)      AS order_ts,
  'online'                                   AS channel,
  e.payment_mode,
  cd.customer_key,
  pd.product_sk,
  e.item_quantity                            AS quantity,
  CASE WHEN e.item_quantity > 0
       THEN CAST(e.item_amount / e.item_quantity AS DECIMAL(10,2))
       ELSE NULL END                         AS unit_price,
  CAST(e.item_amount AS DECIMAL(10,2))       AS total_amount,
  CAST(e.online_customer_id AS STRING)       AS _source_customer_ref,
  upper(trim(e.item_sku_id))                 AS _source_product_ref
FROM exploded e
LEFT JOIN uc_merchandise_lakehouse.silver.customers_dim cd
  ON cd.online_customer_id = e.online_customer_id
LEFT JOIN uc_merchandise_lakehouse.silver.products_dim pd
  ON pd.sku_id = upper(trim(e.item_sku_id))
 AND pd.__END_AT IS NULL;

-- Step 3: Union both channels -------------------------------------------------

CREATE OR REFRESH STREAMING TABLE stg_sales_updates AS
SELECT * FROM STREAM(stg_offline_sales)
UNION ALL
SELECT * FROM STREAM(stg_online_sales);

-- Step 4: Split resolved vs unresolved (quarantine) ---------------------------
-- A null customer_key or sku_id here means the dimension lookup failed —
-- e.g. an order for a customer/product not yet present in Silver, or a
-- product sold outside any tracked __START_AT/__END_AT window.

CREATE OR REFRESH STREAMING TABLE orders_fact AS
SELECT * EXCEPT (_source_customer_ref, _source_product_ref)
FROM STREAM(stg_sales_updates)
WHERE customer_key IS NOT NULL AND product_sk IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE orders_fact_quarantine AS
SELECT
  *,
  CASE
    WHEN customer_key IS NULL AND product_sk IS NULL THEN 'unmatched_customer_and_product'
    WHEN customer_key IS NULL THEN 'unmatched_customer'
    ELSE 'unmatched_product'
  END                   AS _reject_reason,
  current_timestamp()   AS _quarantined_at
FROM STREAM(stg_sales_updates)
WHERE customer_key IS NULL OR product_sk IS NULL;