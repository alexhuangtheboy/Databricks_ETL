CREATE OR REPLACE TEMP VIEW rolling_usage_roaming AS
SELECT
  q2.product_id,
  r.COUNTRY_CODE,
  year,
  month,
  DAY(CURRENT_DATE) AS day,
  SUM((CAST(q1.upFlux AS DOUBLE) + CAST(q1.downFlux AS DOUBLE)) / 1024 / 1024 / 1024) AS usage_GB
FROM
  ods.cdr01 q1
    JOIN ods.resource_res_vsim_product q2
      ON q1.sqb_prodId = q2.product_id
    JOIN simo_prod.ods.resource_res_mcc r
      ON q1.mcc = r.MCC
WHERE
  year = year(current_date)
  AND q2.supplier_id not in (2275,2435,2434,2365)
  AND month = month(current_date)
  AND q2.product_id NOT IN (
    '60011626', '60011749', '60011845', '60011982', '60011984', '60012166', '60012168' --不计费测试卡
  )
  AND local = 0
GROUP BY
  q2.product_id,
  r.COUNTRY_CODE,
  year,
  month,
  day;

CREATE OR REPLACE TEMP VIEW rolling_usage AS
SELECT
  product_id,
  year,
  month,
  SUM((CAST(t1.upFlux AS DOUBLE) + CAST(t1.downFlux AS DOUBLE)) / 1024 / 1024 / 1024) AS usage_GB
FROM
  ods.cdr01 t1
    JOIN ods.resource_res_vsim_product t2
      ON t1.sqb_prodId = t2.product_id
    JOIN simo_prod.ods.resource_res_mcc r
      ON t1.mcc = r.MCC
WHERE
  year = year(current_date)
  AND t2.supplier_id not in (2275,2435,2434,2365)
  AND month = month(current_date)
  AND t2.product_id NOT IN (
    '60011626', '60011749', '60011845', '60011982', '60011984', '60012166', '60012168' --不计费测试卡
  )
GROUP BY
  product_id,
  year,
  month;

CREATE OR REPLACE TEMP VIEW roaming_usage_summary AS
SELECT
  product_id,
  country_code,
  year,
  month,
  day,
  SUM(usage_gb) AS total_usage
FROM
  rolling_usage_roaming
GROUP BY
  product_id,
  COUNTRY_CODE,
  year,
  month,
  day;

CREATE OR REPLACE TEMP VIEW multiple_cycles AS
SELECT
  t1.product_id,
  t2.cycle_time,
  t2.cycle_end_time,
  count(distinct t1.imsi) as num,
  t3.in_flux_package_amount,
  t3.billing_cycle,
  CASE
    WHEN t3.billing_cycle = '1月' THEN DAY(LAST_DAY(CURRENT_DATE()))
    WHEN t3.billing_cycle = '30天' THEN 30
    WHEN t3.billing_cycle = '90天' THEN 90
    WHEN t3.billing_cycle = '28天' THEN 28
  END AS billing_days,
  try_divide(DAY(LAST_DAY(CURRENT_DATE())), billing_days) AS percentage,
  CASE
    WHEN t3.in_flux_package_amount > 60 THEN 60
    ELSE t3.in_flux_package_amount
  END AS package_amount,
  COUNT(DISTINCT t2.imsi) AS card_num,
  case
    when month(t2.cycle_time) = month(current_date) - 1 then day(t2.cycle_end_time)
    else DAY(LAST_DAY(CURRENT_DATE())) - day(t2.cycle_time) + 1
  end as num_days,
  (num_days / billing_days) * package_amount * card_num traffic,
  count(*) over (partition by t1.product_id) cnt
FROM
  ods.resource_res_vsim_billing t1
    JOIN ods.resource_res_vsim_info t2
      ON t1.imsi = t2.imsi
    JOIN ods.resource_res_vsim_product t3
      ON t1.product_id = t3.product_id
WHERE
  YEAR(t2.cycle_time) = year(current_date)
  AND (
    (
      MONTH(t2.cycle_time) = month(current_date) - 1
      AND MONTH(t2.cycle_end_time) = month(current_date)
    )
    OR (
      MONTH(t2.cycle_time) = month(current_date)
      AND MONTH(t2.cycle_end_time) IN (month(current_date), month(current_date) + 1)
    )
  )
  AND t1.product_id NOT IN (
    '60011626', '60011749', '60011845', '60011982', '60011984', '60012166', '60012168'
  )
  AND t3.supplier_id not in (2275,2435,2434,2365)
group by
  t1.product_id,
  t2.cycle_time,
  t2.cycle_end_time,
  t3.in_flux_package_amount,
  t3.billing_cycle;

CREATE OR REPLACE TEMP VIEW rolling_traffic AS
SELECT
  T1.product_id,
  t3.in_flux_package_amount,
  t3.billing_cycle,
  CASE
    WHEN t3.billing_cycle = '1月' THEN DAY(LAST_DAY(CURRENT_DATE()))
    WHEN t3.billing_cycle = '30天' THEN 30
    WHEN t3.billing_cycle = '90天' THEN 90
    WHEN t3.billing_cycle = '28天' THEN 28
  END AS billing_days,
  try_divide(DAY(LAST_DAY(CURRENT_DATE())), billing_days) AS percentage,
  CASE
    WHEN t3.in_flux_package_amount > 60 THEN 60
    ELSE t3.in_flux_package_amount
  END AS package_amount,
  COUNT(DISTINCT t2.imsi) AS card_num,
  package_amount * card_num * percentage AS fixed_purchased_traffic
FROM
  ods.resource_res_vsim_billing t1
  JOIN ods.resource_res_vsim_info t2 ON t1.imsi = t2.imsi
  JOIN ods.resource_res_vsim_product t3 ON t1.product_id = t3.product_id
WHERE 
  YEAR(t2.cycle_time) = year(current_date)
  AND (
    (
      MONTH(t2.cycle_time) = month(current_date) - 1
      AND MONTH(t2.cycle_end_time) = month(current_date)
    )
    OR (
      MONTH(t2.cycle_time) = month(current_date)
      AND MONTH(t2.cycle_end_time) IN (month(current_date), month(current_date) + 1)
    )
  )
  AND t1.product_id NOT IN (
    '60011626',
    '60011749',
    '60011845',
    '60011982',
    '60011984',
    '60012166',
    '60012168'
  )
  AND t3.supplier_id not in (2275,2435,2434,2365)
  AND t1.product_id not in (
    SELECT
      product_id
    FROM
      multiple_cycles
    where
      cnt > 2
  )
GROUP BY
  T1.product_id,
  t3.in_flux_package_amount,
  t3.billing_cycle
union
select
  product_id,
  in_flux_package_amount,
  billing_cycle,
  billing_days,
  percentage,
  package_amount,
  sum(card_num) card_num,
  sum(traffic) fixed_purchased_traffic
from
  (
    SELECT
      product_id,
      cycle_time,
      cycle_end_time,
      in_flux_package_amount,
      billing_cycle,
      billing_days,
      percentage,
      package_amount,
      card_num,
      num_days,
      traffic
    FROM
      multiple_cycles
    where
      cnt > 2
      AND current_date between date_format(cycle_time, 'yyyy-MM-dd')
      and date_format(cycle_end_time, 'yyyy-MM-dd')
    UNION
    SELECT
      H.product_id,
      cycle_time,
      next_cycle_time cycle_end_time,
      in_flux_package_amount,
      billing_cycle,
      billing_days,
      percentage,
      C.package_amount,
      count(distinct H.imsi) as card_num,
      day(H.next_cycle_time) -1 num_days,
      (num_days / billing_days) * package_amount * card_num AS traffic
    FROM
      ods.resource_res_vsim_cycle_history H
      left join (
        select
          distinct product_id,
          in_flux_package_amount,
          billing_cycle,
          billing_days,
          percentage,
          package_amount
        from
          multiple_cycles
      ) as C on H.product_id = C.product_id
    where
      H.product_id in (
        select
          product_id
        from
          multiple_cycles
        where
          cnt > 2
      )
      and year(cycle_time) = year(current_date)
      and month(cycle_time) = month(current_date) - 1
      and month(next_cycle_time) = month(current_date)
    group by
      H.product_id,
      H.cycle_time,
      H.next_cycle_time,
      in_flux_package_amount,
      billing_cycle,
      billing_days,
      percentage,
      C.package_amount
  )as B
group by
  product_id,
  in_flux_package_amount,
  billing_cycle,
  billing_days,
  percentage,
  package_amount;

CREATE OR REPLACE TEMP VIEW rolling_utilization_roaming AS
SELECT
  A.product_id,
  A.country_code,
  A.year,
  A.month,
  A.day,
  A.total_usage AS country_usage,
  U.usage_GB AS total_usage,
  CASE
    WHEN T.fixed_purchased_traffic = 0 THEN A.total_usage
    ELSE T.fixed_purchased_traffic
  END AS fixed_purchased_traffic,
  COALESCE(try_divide(U.usage_GB, T.fixed_purchased_traffic), 1) AS percentage,
  T.card_num
FROM
  roaming_usage_summary A
    LEFT JOIN rolling_usage U
      ON A.product_id = U.product_Id
    LEFT JOIN rolling_traffic T
      ON A.product_id = T.product_id
WHERE
  A.product_id IN (
    SELECT
      T.product_id
    FROM
      rolling_traffic T
  );

CREATE OR REPLACE TEMP VIEW usage AS
SELECT
  q2.product_id,
  r.COUNTRY_CODE,
  year,
  month,
  DAY(CURRENT_DATE) AS day,
  SUM((CAST(q1.upFlux AS DOUBLE) + CAST(q1.downFlux AS DOUBLE)) / 1024 / 1024 / 1024) AS usage_GB,
  RANK() OVER (
      PARTITION BY q2.product_id
      ORDER BY
        SUM((CAST(q1.upFlux AS DOUBLE) + CAST(q1.downFlux AS DOUBLE)) / 1024 / 1024 / 1024) DESC
    ) AS rnk
FROM
  ods.cdr01 q1
    JOIN ods.resource_res_vsim_product q2
      ON q1.sqb_prodId = q2.product_id
    JOIN simo_prod.ods.resource_res_mcc r
      ON q1.mcc = r.MCC
WHERE
  year = year(current_date)
  AND q2.supplier_id not in (2275,2435,2434,2365)
  AND month = month(current_date)
  AND q2.product_id NOT IN (
    '60011626', '60011749', '60011845', '60011982', '60011984', '60012166', '60012168' --不计费测试卡
  )
  AND local = 1
GROUP BY
  q2.product_id,
  r.COUNTRY_CODE,
  year,
  month,
  day;

CREATE OR REPLACE TEMP VIEW rolling_usage_local AS
SELECT
  product_id,
  year,
  month,
  day,
  SUM(usage_GB) AS total_usage_GB,
  MAX(
    CASE
      WHEN rnk = 1 THEN COUNTRY_CODE
    END
  ) AS top_country
FROM
  usage
GROUP BY
  product_id,
  year,
  month,
  day;

CREATE OR REPLACE TEMP VIEW rolling_utilization_summary AS
SELECT
  product_id,
  country_code,
  year,
  month,
  day,
  country_usage AS usage,
  country_usage / percentage AS traffic,
  card_num
FROM
  rolling_utilization_roaming
UNION ALL
SELECT
  L.product_id,
  L.TOP_COUNTRY,
  L.year,
  L.month,
  L.day,
  total_usage_gb,
  fixed_purchased_traffic,
  card_num
FROM
  rolling_usage_local L
    LEFT JOIN rolling_traffic T
      ON L.product_id = T.product_id;

INSERT INTO simo_prod.db_alex.rolling_utilization
SELECT
  product_id,
  usage AS usage_GB,
  traffic AS traffic_GB,
  CASE 
    WHEN product_id IN ('60011417', '60011613') THEN 'GU'
    ELSE S.country_code
  END AS country_code,
  CASE 
    WHEN product_id IN ('60011417', '60011613') THEN 'Guam & Saipan'
    ELSE R.description
  END AS description,
  CASE 
    WHEN product_id IN ('60011417', '60011613') THEN '关岛塞班'
    ELSE R.country_name
  END AS country_name,
  year,
  month,
  day - 1 AS day
FROM
  rolling_utilization_summary S
LEFT JOIN (
  SELECT DISTINCT
    country_code,
    description,
    country_name
  FROM
    simo_prod.ods.resource_res_mcc
) R
  ON S.country_code = R.country_code;