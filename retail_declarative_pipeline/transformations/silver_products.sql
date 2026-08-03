-- Step 1: Standardize offline products ---------------------------------------
-- Casing convention: product_name/category -> Title Case, brand -> UPPER.
-- (Adjust if you'd rather standardize differently — this is a style choice,
-- not a functional requirement; what matters is both sources land the same way.)
 
CREATE OR REFRESH STREAMING TABLE stg_offline_products AS
SELECT
  upper(trim(sku_id))                    AS sku_id,
  CAST(product_id AS INT)                AS offline_product_id,
  initcap(trim(lower(product_name)))     AS product_name,
  initcap(trim(lower(category)))         AS category,
  upper(trim(brand))                     AS brand,
  CAST(insert_timestamp AS TIMESTAMP)         AS _source_ts,
  1                                       AS source_priority
FROM STREAM(uc_merchandise_lakehouse.bronze.offline_products);
 
-- Step 2: Standardize online products (embedded per order line item) --------
-- `items` is an array — explode it first so each item becomes its own row.
 
CREATE OR REFRESH STREAMING TABLE stg_online_products AS
SELECT
  upper(trim(item.sku_id))               AS sku_id,
  CAST(NULL AS INT)                      AS offline_product_id,
  initcap(trim(lower(item.product_name))) AS product_name,
  initcap(trim(lower(item.category)))    AS category,
  upper(trim(item.brand))                AS brand,
  CAST(order_timestamp AS TIMESTAMP)     AS _source_ts,
  2                                       AS source_priority
FROM STREAM(uc_merchandise_lakehouse.bronze.online_sales_rawdata)
LATERAL VIEW explode(items) item_tbl AS item;
 
-- Step 3: Union + composite sequence key -------------------------------------
-- IMPORTANT: unlike customer_dim's string-based _seq_key, this one must stay
-- a real TIMESTAMP. STORED AS SCD TYPE 2 populates __START_AT/__END_AT with
-- this same type — and the Sales Fact needs to range-join real order
-- timestamps against __START_AT/__END_AT for point-in-time product lookups.
-- Priority is encoded as a microsecond-scale nudge instead of a string
-- prefix: negligible for ordering vs. real timestamps, but still guarantees
-- online (priority 2) sorts after offline (priority 1) on an exact tie.
 
CREATE OR REFRESH STREAMING TABLE stg_product_updates AS
SELECT
  *,
  _source_ts + (source_priority * INTERVAL 1 MICROSECONDS) AS _seq_key
FROM (
  SELECT * FROM STREAM(stg_offline_products)
  UNION ALL
  SELECT * FROM STREAM(stg_online_products)
);
 
-- Step 4: Split valid vs invalid (quarantine), and compute the surrogate key --
-- Delta IDENTITY (auto-increment) columns are NOT supported on AUTO CDC
-- target tables, so we can't let the engine auto-assign a surrogate key.
-- Instead, product_sk is a deterministic hash of (sku_id, _seq_key) — unique
-- per version, stable across reruns (same inputs always hash the same), and
-- computed here in the staging table so it flows straight through as a
-- normal column via AUTO CDC INTO.
 
CREATE OR REFRESH STREAMING TABLE stg_product_valid AS
SELECT
  *,
  sha2(concat_ws('|', sku_id, CAST(_seq_key AS STRING)), 256) AS product_sk
FROM STREAM(stg_product_updates)
WHERE sku_id IS NOT NULL AND length(sku_id) > 0;
 
CREATE OR REFRESH STREAMING TABLE products_quarantine AS
SELECT
  *,
  'missing_sku_id'     AS _reject_reason,
  current_timestamp()  AS _quarantined_at
FROM STREAM(stg_product_updates)
WHERE sku_id IS NULL OR length(sku_id) = 0;
 
-- Step 5: AUTO CDC INTO the final SCD2 product dimension ---------------------
-- STORED AS SCD TYPE 2 gives you __START_AT / __END_AT automatically, using
-- the same type as _seq_key (TIMESTAMP here). A new version row is inserted
-- whenever a tracked column changes; TRACK HISTORY ON * (the default) means
-- any attribute change — product_name, category, or brand — creates history.
 
CREATE OR REFRESH STREAMING TABLE products_dim;
 
CREATE FLOW product_dim_flow AS AUTO CDC INTO products_dim
FROM STREAM(stg_product_valid)
KEYS (sku_id)
IGNORE NULL UPDATES
SEQUENCE BY _seq_key
COLUMNS * EXCEPT (source_priority, _seq_key, _source_ts)
STORED AS SCD TYPE 2
TRACK HISTORY ON product_name, category, brand;