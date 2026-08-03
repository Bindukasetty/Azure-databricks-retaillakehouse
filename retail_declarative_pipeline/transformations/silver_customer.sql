--standardize offline customers--

create or refresh streaming table stg_offline_customers as
select 
    CASE
    WHEN length(regexp_replace(phone_number,'[^0-9]',''))>= 10
    THEN substring(regexp_replace(phone_number,'[^0-9]',''),-10,10)
    else length(regexp_replace(phone_number,'[^0-9]','')) END AS customer_key,
  CAST(customer_id AS INT)    AS offline_customer_id,
  CAST(NULL AS STRING)        AS online_customer_id,
  trim(customer_name)         AS customer_name,
  lower(trim(email))          AS email,
  trim(city)                  AS city,
  CAST(insert_timestamp AS TIMESTAMP) AS _source_ts,
  1                            AS source_priority
FROM STREAM(uc_merchandise_lakehouse.bronze.offline_customers);

--standardize online customers--
-- DROP TABLE IF EXISTS  stg_online_customers;
create or refresh streaming table stg_online_customers as
select 
    CASE
    WHEN length(regexp_replace(customer.phone,'[^0-9]',''))>= 10
    THEN substring(regexp_replace(customer.phone,'[^0-9]',''),-10,10)
    else length(regexp_replace(customer.phone,'[^0-9]','')) END AS customer_key,
  CAST(NULL AS STRING)        AS offline_customer_id,
  CAST(customer.customer_id AS string)    AS online_customer_id,
  trim(customer.name)         AS customer_name,
  lower(trim(customer.email))          AS email,
  CAST(NULL AS STRING)                 AS city,
  CAST(order_timestamp AS TIMESTAMP) AS _source_ts,
  2                            AS source_priority
FROM STREAM(uc_merchandise_lakehouse.bronze.online_sales_rawdata);

--merge offline and online customers--

create or refresh streaming table stg_customers as
SELECT
  *,
  concat(lpad(source_priority, 2, '0'), date_format(_source_ts, 'yyyyMMddHHmmssSSS')) AS _seq_key
FROM (
select * from stream (stg_offline_customers)
union all
select * from stream( stg_online_customers)
);

--split valid vs invalid records(quarantine table)
create or refresh streaming table stg_customers_valid as
select * from stream(stg_customers)
where length(customer_key)=10;

create or refresh streaming table customers_quarantine as
select * ,
'invalid_phone_number' as reject_reason,
current_timestamp() as quarantined_at
from stream(stg_customers)
where length(customer_key)!=10;


--implementing scd1 for customers

create or refresh streaming table customers_dim;

create flow customers_dim_flow as auto cdc into customers_dim
from stream(stg_customers_valid)
keys(customer_key)
ignore null updates
sequence by _seq_key
columns * except(source_priority,_seq_key,_source_ts)
stored as scd type 1





  