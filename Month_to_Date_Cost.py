#Import all the packages 

%pip install pymysql
import pymysql
import pandas as pd
from pyspark.sql.functions import split
from pyspark.sql import SparkSession
from decimal import Decimal
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, TimestampType, DecimalType
from datetime import datetime, timedelta
from pyspark.sql.functions import decimal
spark.conf.set("spark.sql.ansi.enabled", False)


  # Retrieve all the pooling product items and its data usage by searching all the items without prices from resource_res_vsim_product
  
  pooling_products = f"""
  with regular as (
  SELECT
    t1.product_id,
    t2.cycle_time,
    t2.cycle_end_time,
    count(distinct t1.imsi) as num,
    t3.in_flux_package_amount,
    t3.billing_cycle,
    t3.package_price,
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
    simo_prod.ods.resource_res_vsim_billing t1
      JOIN simo_prod.ods.resource_res_vsim_info t2
        ON t1.imsi = t2.imsi
      JOIN simo_prod.ods.resource_res_vsim_product t3
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
    AND t3.package_type=0
  group by
    t1.product_id,
    t2.cycle_time,
    t2.cycle_end_time,
    t3.in_flux_package_amount,
    t3.billing_cycle,
    t3.package_price)

  select
    *,
    concat(product_id,'_',country_code) as find_item
  from simo_prod.db_alex.rolling_utilization
  where make_date(year,month,day)=curdate()-1 and product_id not in (select product_id from regular) """

  rows = spark.sql(pooling_products).toPandas()

  # Build the dictionary
  result = {f"{row['find_item']}": row['traffic_GB'] for _, row in rows.iterrows()}


  # Loop through the dictionary
  for key, value in result.items():
      print(key, value)

# Retrieve all the pooling product items by searching all the items from the table resource_res_product_fee_list and perform the price calculation

pooling_prices=f""" select
  concat(result.product_id,'_',result.COUNTRY_CODE) as search_item
  ,result.product_id,
  result.COUNTRY_CODE,
  result.start_val,result.finish_val,
  avg(result.flux_fee) flux_fee
from
(select
  A.product_id,
  A.mcc,
  case when A.flux_unit='MB' then  P.settle_accounts_rate*A.flux_fee*1024
  else P.settle_accounts_rate*A.flux_fee
  end as flux_fee,
  case 
  when A.flux_start_val<0 then 0
  else A.flux_start_val
  end as start_val,
  case 
  when A.flux_finish_val<0 then 999999999
  else A.flux_finish_val
  end as finish_val,
  'GB' as flux_unit,
   M.COUNTRY_CODE,
   dense_rank() over(partition by A.product_id, M.COUNTRY_CODE,A.flux_start_val order by flux_fee desc) RNK
from
(select
  product_id,
  mcc,
  flux_fee,
  flux_start_val,
  flux_finish_val,
  flux_finish_val,
  flux_unit
from
  simo_prod.ods.resource_res_product_fee_list L
  lateral view explode(split(mccs, ',')) addtable as mcc)as A
left join simo_prod.ods.resource_res_mcc M
on A.mcc=M.MCC
left join simo_prod.ods.resource_res_vsim_product P
on A.product_id=P.product_id
)as result
where result.RNK=1
group by concat(result.product_id,'_',result.COUNTRY_CODE),result.product_id,result.COUNTRY_CODE,result.start_val,result.finish_val
"""
items=spark.sql(pooling_prices).toPandas()

result_dict = {
    row['search_item']: [row['start_val'], row['finish_val'], row['flux_fee']]
    for _, row in items.iterrows()
}


import pandas as pd
from collections import defaultdict
from typing import Dict, List, Union


class PoolingFeeCalculator:
    """
    Class to calculate total fees for pooling products based on usage and tiered pricing rules.
    """

    def __init__(self, tier_items: pd.DataFrame, usage_dict: Dict[str, float]):
        """
        Initialize with tiering rules and usage data.

        Args:
            tier_items (pd.DataFrame): DataFrame containing tier rules with columns:
                ['search_item', 'start_val', 'finish_val', 'flux_fee']
            usage_dict (dict): Dictionary mapping 'search_item' to usage value
        """
        self.tier_items = tier_items
        self.usage_dict = usage_dict
        self.tier_dict = self._build_tier_dict()

    def _build_tier_dict(self) -> Dict[str, List[List[Union[float, int]]]]:
        """
        Build a dictionary mapping each search_item to a list of tiers.
        """
        tier_dict = defaultdict(list)
        for _, row in self.tier_items.iterrows():
            key = row['search_item']
            tier_dict[key].append([row['start_val'], row['finish_val'], row['flux_fee']])
        return tier_dict

    def calculate_fees(self) -> pd.DataFrame:
        """
        Calculate total fees for each product-country based on usage and tier rules.

        Returns:
            pd.DataFrame: DataFrame with columns ['product_id', 'country_code', 'cost', 'Pooling']
        """
        results = []

        # Only calculate for items that exist in both usage and tier rules
        for key in self.usage_dict.keys() & self.tier_dict.keys():
            usage = self.usage_dict[key]
            tiers = sorted(self.tier_dict[key], key=lambda x: x[0])  # sort by start_val

            remaining = usage
            total_fee = 0

            for start_val, finish_val, flux_fee in tiers:
                if remaining <= 0:
                    break

                # Calculate overlap of usage with current tier
                used_in_tier = min(remaining, finish_val - start_val)
                if usage > start_val:
                    total_fee += used_in_tier * flux_fee
                    remaining -= used_in_tier

            results.append({
                'productid_country': key,
                'traffic': usage,
                'total_fee': total_fee
            })

            print(f"{key}, traffic: {usage}, total fee: {total_fee}")

        df = pd.DataFrame(results)
        # Only keep rows with positive fee
        irregular_products = df.query('total_fee > 0').copy()

        # Split the 'key' column into product_id and country_code
        irregular_products[['product_id', 'country_code']] = irregular_products['productid_country'].str.split('_', expand=True)
        irregular_products = irregular_products[['product_id', 'country_code', 'total_fee']].rename(
            columns={'total_fee': 'cost'}
        )
        irregular_products['Pooling'] = 'Pooling'

        return irregular_products

    def display(self) -> None:
        """
        Calculate fees and display the final DataFrame.
        """
        df = self.calculate_fees()
        display(df)

# Suppose 'items' is your tier DataFrame and 'result' is your usage dictionary
calculator = PoolingFeeCalculator(tier_items=items, usage_dict=result)
irregular_products = calculator.calculate_fees()

# Display in Databricks
display(irregular_products)


# Retrive all the regular products cost(exluding pooling products) from the database

regular_products = f"""  
  with multiple_cycles AS
(SELECT
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
  (num_days / billing_days) * card_num * case 
    when t3.balance_currency='USD' then coalesce(t3.package_price_withusd,t3.package_price*t3.balance_rate)
    else coalesce(t3.package_price_withusd,t3.package_price*t3.settle_accounts_rate)
  end as cost,
  count(*) over (partition by t1.product_id) cnt
FROM
  simo_prod.ods.resource_res_vsim_billing t1
    JOIN simo_prod.ods.resource_res_vsim_info t2
      ON t1.imsi = t2.imsi
    JOIN simo_prod.ods.resource_res_vsim_product t3
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
  t3.billing_cycle,
  case 
    when t3.balance_currency='USD' then coalesce(t3.package_price_withusd,t3.package_price*t3.balance_rate)
    else coalesce(t3.package_price_withusd,t3.package_price*t3.settle_accounts_rate)
  end ),

products_A as (select
  product_id,
  fixed_purchased_traffic as traffic,
  cost
from
(SELECT
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
  case
    when month(t2.cycle_time) = month(current_date) - 1 then day(t2.cycle_end_time)
    else DAY(LAST_DAY(CURRENT_DATE())) - day(t2.cycle_time) + 1
  end as num_days,
  package_amount * card_num * percentage AS fixed_purchased_traffic,
  percentage * card_num * case 
    when t3.balance_currency='USD' then coalesce(t3.package_price_withusd,t3.package_price*t3.balance_rate)
    else coalesce(t3.package_price_withusd,t3.package_price*t3.settle_accounts_rate)
  end as cost
FROM
  simo_prod.ods.resource_res_vsim_billing t1
  JOIN simo_prod.ods.resource_res_vsim_info t2 ON t1.imsi = t2.imsi
  JOIN simo_prod.ods.resource_res_vsim_product t3 ON t1.product_id = t3.product_id
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
  t2.cycle_time,
  t2.cycle_end_time,
  t3.in_flux_package_amount,
  t3.billing_cycle,
  case 
    when t3.balance_currency='USD' then coalesce(t3.package_price_withusd,t3.package_price*t3.balance_rate)
    else coalesce(t3.package_price_withusd,t3.package_price*t3.settle_accounts_rate)
  end 
having fixed_purchased_traffic>0)as Final),

products_B as (
  select
  product_id,
  sum(traffic) tarffic,
  sum(cost) cost
from
(SELECT
  M.product_id,
  M.cycle_time,
  M.cycle_end_time,
  M.in_flux_package_amount,
  M.billing_cycle,
  M.billing_days,
  M.percentage,
  M.package_amount,
  M.card_num,
  M.num_days,
  M.traffic,
  (num_days / billing_days) * card_num * case 
    when t1.balance_currency='USD' then coalesce(t1.package_price_withusd,t1.package_price*t1.balance_rate)
    else coalesce(t1.package_price_withusd,t1.package_price*t1.settle_accounts_rate)
  end as cost 
FROM
  multiple_cycles M
left join simo_prod.ods.resource_res_vsim_product t1
on M.product_id=t1.product_id
where
  cnt > 2
  AND current_date between
    date_format(cycle_time, 'yyyy-MM-dd')
  and
    date_format(cycle_end_time, 'yyyy-MM-dd')
    and M.traffic>0
UNION
SELECT
  H.product_id,
  cycle_time,
  next_cycle_time cycle_end_time,
  C.in_flux_package_amount,
  C.billing_cycle,
  C.billing_days,
  C.percentage,
  C.package_amount,
  count(distinct H.imsi) as card_num,
  day(H.next_cycle_time) - 1 num_days,
  (num_days / billing_days) * C.package_amount * card_num AS traffic,
  (num_days / billing_days) * card_num * case 
    when t2.balance_currency='USD' then coalesce(t2.package_price_withusd,t2.package_price*t2.balance_rate)
    else coalesce(t2.package_price_withusd,t2.package_price*t2.settle_accounts_rate)
  end as cost
FROM
  simo_prod.ods.resource_res_vsim_cycle_history H
    left join (
      select distinct
        product_id,
        in_flux_package_amount,
        billing_cycle,
        billing_days,
        percentage,
        package_amount
      from
        multiple_cycles
    ) as C
      on H.product_id = C.product_id
    left join simo_prod.ods.resource_res_vsim_product t2
  on t2.product_id=H.product_id
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
  C.in_flux_package_amount,
  C.billing_cycle,
  C.billing_days,
  percentage,
  C.package_amount,
  case 
    when t2.balance_currency='USD' then coalesce(t2.package_price_withusd,t2.package_price*t2.balance_rate)
    else coalesce(t2.package_price_withusd,t2.package_price*t2.settle_accounts_rate)
  end
having traffic>0)
group by product_id
),

regular as (select * from products_A
union 
select * from products_B)

,
--Split the products with different country codes to each country with its corresponding cost 

special_products as(select
  special_products.product_id,
  special_products.country_code,
  percentage*cost cost
from
(select
  U.product_id,
  U.traffic_GB,
  final_reference.total_traffic,
  U.traffic_GB/final_reference.total_traffic as percentage,
  U.country_code,
  U.description,
  U.year,
  U.month,
  U.day
from
  simo_prod.db_alex.rolling_utilization U
join 
(select
  distinct product_id,
  reference.total_traffic
from
(select *,dense_rank() over(partition by product_id order by country_code) rnk, sum(traffic_gb) over(partition by product_id) total_traffic from simo_prod.db_alex.rolling_utilization where make_date(year,month,day)=curdate()-1 and product_id in (select product_id from regular))as reference
where rnk>1)as final_reference
on U.product_id=final_reference.product_id
where make_date(year,month,day)=curdate()-1 and U.product_id in (select product_id from regular))as special_products
left join (select product_id, sum(cost) cost from regular group by product_id) as regular on special_products.product_id=regular.product_id)

select
  regular.product_id,
  C.country_code,
  regular.cost
from
  regular
left join (select product_id, country_code from simo_prod.db_alex.rolling_utilization where make_date(year,month,day)=curdate()-1 ) C
on regular.product_id=C.product_id
where regular.product_id not in (select product_id from special_products)
union all
select
  product_id,  
  country_code,
  cost
from
  special_products
 """
regular_products = spark.sql(regular_products).toPandas()
regular_products['Pooling']='Regular'
all_products=pd.concat([regular_products,irregular_products])

# Get yesterday's date in the yyyy-MM-dd format

yesterday = datetime.now() - timedelta(days=1)
all_products['period']=yesterday.strftime('%Y-%m-%d')
all_products['period']=pd.to_datetime(all_products['period'], format='%Y-%m-%d')
display(all_products)


