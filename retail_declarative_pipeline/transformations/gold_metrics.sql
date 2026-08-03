-- Gold Layer — Reporting Views
-- Lakeflow Declarative Pipeline (SQL)

-- Q1: What is total revenue by channel? ---------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold.gold_revenue_by_channel AS
SELECT
  channel,
  SUM(total_amount)          AS total_revenue,
  COUNT(DISTINCT order_id)   AS order_count,
  SUM(quantity)               AS total_units_sold
FROM orders_fact
GROUP BY channel;

-- Q2: Which products generate the highest revenue? ---------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold.gold_product_performance AS
SELECT
  pd.sku_id,
  pd.product_name,
  pd.category,
  pd.brand,
  SUM(sf.total_amount)        AS total_revenue,
  SUM(sf.quantity)             AS total_units_sold,
  COUNT(DISTINCT sf.order_id)  AS order_count
FROM orders_fact sf
JOIN products_dim pd ON pd.product_sk = sf.product_sk
GROUP BY pd.sku_id, pd.product_name, pd.category, pd.brand;
-- Note: no ORDER BY / LIMIT here — "top N" is a query-time concern, not a
-- storage concern. Rank by total_revenue DESC when querying this view.

-- Q3: Who are the highest-value customers? ------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold.gold_customer_value AS
SELECT
  cd.customer_key,
  cd.customer_name,
  cd.email,
  cd.city,
  SUM(sf.total_amount)         AS lifetime_value,
  COUNT(DISTINCT sf.order_id)  AS order_count,
  MIN(sf.order_ts)             AS first_order_ts,
  MAX(sf.order_ts)             AS last_order_ts
FROM orders_fact sf
JOIN customers_dim cd ON cd.customer_key = sf.customer_key
GROUP BY cd.customer_key, cd.customer_name, cd.email, cd.city;

-- Q4: What percentage of customers purchase through both channels? -----------

CREATE OR REFRESH MATERIALIZED VIEW gold.gold_customer_channel_mix AS
SELECT
  customer_key,
  COUNT(DISTINCT channel)                                   AS channels_used,
  CASE WHEN COUNT(DISTINCT channel) = 2 THEN TRUE ELSE FALSE END AS purchased_both_channels
FROM sales_fact
GROUP BY customer_key;

CREATE OR REFRESH MATERIALIZED VIEW gold.gold_cross_channel_summary AS
SELECT
  COUNT(*)                                                          AS total_customers,
  SUM(CASE WHEN purchased_both_channels THEN 1 ELSE 0 END)          AS both_channel_customers,
  ROUND(
    100.0 * SUM(CASE WHEN purchased_both_channels THEN 1 ELSE 0 END) / COUNT(*),
    2
  )                                                                  AS pct_both_channels
FROM gold.gold_customer_channel_mix;

-- Q5: What are the top-performing product categories? -------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold.gold_category_performance AS
SELECT
  pd.category,
  SUM(sf.total_amount)        AS total_revenue,
  SUM(sf.quantity)             AS total_units_sold,
  COUNT(DISTINCT sf.order_id)  AS order_count
FROM orders_fact sf
JOIN products_dim pd ON pd.product_sk = sf.product_sk
GROUP BY pd.category;

-- Q6: How do online and offline sales compare over time? ----------------------

CREATE OR REFRESH MATERIALIZED VIEW gold.gold_channel_trend_daily AS
SELECT
  CAST(order_ts AS DATE)     AS order_date,
  channel,
  SUM(total_amount)           AS daily_revenue,
  COUNT(DISTINCT order_id)    AS daily_order_count
FROM orders_fact
GROUP BY CAST(order_ts AS DATE), channel;