# Import all the packages as needed
import pymysql
import pandas as pd
from pyspark.sql.functions import split
from pyspark.sql import SparkSession
from decimal import Decimal
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
    DecimalType,
)
from datetime import datetime, timedelta
from pyspark.sql.functions import decimal

spark.conf.set("spark.sql.ansi.enabled", False)

rsim_summary=f"""
with CTE as (
  select distinct
    Territory,
    Network,
    `MCC MNC` as plmn,
    `Data/MB (USD)` as JT_price
  from
    simo_prod.db_alex.jersey_telecom_price
),

CTA as(select
    coalesce(Territory, `Country/Area`) as Country,
    coalesce(Network, Operator) as Carrier,
    coalesce(plmn,mcc_mnc ) as PLMN,
    JT_price,
    JoyTel_price,
    Network_Availbility
from
(select
  *
from
  CTE C
full outer join (
select
  `Country/Area`,
  Operator,
  mcc_mnc,
  `Network（NSA 5G only）` Network_Availbility,
  round(price * 0.13,5) JoyTel_price
from
  simo_prod.db_alex.joytel_price
  lateral view explode(split(mccmnc, '/')) addtable as mcc_mnc
    ) as B
      on C.plmn = B.MCC_MNC)as final)

select cta.Country, 
    R.COUNTRY_CODE,
    cta.PLMN,
    cta.Carrier,
    JT_price,
    JoyTel_price,
    Network_Availbility
 from CTA
left join (select distinct country,COUNTRY_CODE from simo_prod.db_alex.rsim_summary) R
ON CTA.Country = R.country

"""

pccw_simo=f"""
select
  case
    when Country = 'United States' then 'America'
    when Country = 'British Virgin Islands' then 'Virgin Islands British'
    when Country = 'Hong Kong' then 'CN HongKong'
    when Country = 'Taiwan' then 'CN Taiwan'
    else Country
  end as Country,
  case
    when Country = 'United States' then 'US'
    when Country = 'British Virgin Islands' then 'VG'
    when Country = 'Hong Kong' then 'HK'
    when Country = 'Taiwan' then 'TW'
    when country = 'Saint Lucia' then 'LC'
    else C.country_code
  end as Country_code,
  `Network `,
  `Data (MB)`
from
  simo_prod.db_alex.simo_pccw P
    left join (
      select distinct
        DESCRIPTION,
        country_code,
        MCC
      from
        simo_prod.ods.resource_res_mcc
    ) C
      on P.country = C.DESCRIPTION
"""
rsim_summary = spark.sql(rsim_summary).toPandas()
pccw_simo= spark.sql(pccw_simo).toPandas()
display(rsim_summary)
display(pccw_simo)

import pandas as pd
from rapidfuzz import process, fuzz

def exclude(country, carrier_name):
    if pd.isna(carrier_name) or pd.isna(country):
        return carrier_name
    if country in carrier_name:
        carrier_name = carrier_name.replace(country, '').strip()
    return carrier_name

def fuzzy_match(x, choices, scorer=fuzz.ratio, cutoff=50):
    """Return best match string + score, or (None, None)."""
    match = process.extractOne(x, choices, scorer=scorer)
    if match and match[1] >= cutoff:
        return match[0], match[1]
    return (None, None)

# -----------------------------
# Preprocess
# -----------------------------
rsim_summary['fixed_carrier'] = rsim_summary.apply(
    lambda row: exclude(row['Country'], row['Carrier']),
    axis=1
)
rsim_summary['fixed_carrier'] = rsim_summary.apply(
    lambda row: exclude(row['COUNTRY_CODE'], row['fixed_carrier']),
    axis=1
)

rsim_summary['fixed_carrier'] = rsim_summary.apply(
    lambda row: exclude('Communications', row['fixed_carrier']),
    axis=1
)

pccw_simo['fixed_network'] = pccw_simo.apply(
    lambda row: exclude(row['Country'], row['Network ']),
    axis=1
)
pccw_simo['fixed_network'] = pccw_simo.apply(
    lambda row: exclude(row['Country_code'], row['fixed_network']),
    axis=1
)

pccw_simo['fixed_network'] = pccw_simo.apply(
    lambda row: exclude('Communications Inc. ', row['fixed_network']),
    axis=1
)
pccw_simo['fixed_network'] = pccw_simo.apply(
    lambda row: exclude('Reliance', row['fixed_network']),
    axis=1
)

best_matches = []
scores = []
matched_rsim_rows = []

for _, row in pccw_simo.iterrows():
    subset = rsim_summary[rsim_summary['COUNTRY_CODE'] == row['Country_code']]
    
    if not subset.empty:
        best_carrier, score = fuzzy_match(
            row['fixed_network'].lower(), 
            subset['fixed_carrier'].str.lower()
        )
        if best_carrier:
            # lookup the *full PLMN row* that matched
            matched_row = subset[subset['fixed_carrier'].str.lower() == best_carrier]
            if not matched_row.empty:
                matched_rsim_rows.append(matched_row.iloc[0].to_dict())
            else:
                matched_rsim_rows.append({})
        else:
            matched_rsim_rows.append({})
    else:
        best_carrier, score = (None, None)
        matched_rsim_rows.append({})
    
    best_matches.append(best_carrier)
    scores.append(score)

# Attach results back
pccw_simo['best_match'] = best_matches
pccw_simo['match_score'] = scores
import pandas as pd


df = pd.DataFrame(matched_rsim_rows)
rsim_df = df[['COUNTRY_CODE','Carrier']]


# Merge extra PLMN columns side-by-side
result = pd.concat([pccw_simo.reset_index(drop=True), rsim_df.reset_index(drop=True)], axis=1)

result=(result[['Country','Country_code','Network ','Data (MB)','Carrier','COUNTRY_CODE']].drop_duplicates())
display(result)

merged = pd.merge(
    rsim_summary,
    result,  # only keep these cols from result
    how='outer',
    on=['COUNTRY_CODE', 'Carrier']
)

merged = pd.merge(
    rsim_summary,
    result,  # only keep these cols from result
    how='outer',
    on=['COUNTRY_CODE', 'Carrier']
)

merged=merged.drop(columns=['fixed_carrier'])
merged = merged.rename(columns={'Country_x':'Country','Country_y': 'country_pccw','Country_code':'country_code_pccw','Network ':'Carrier_PCCW','Data (MB)':'Date_Price'})
spark.createDataFrame(merged).write.option("mergeSchema", "true").mode("overwrite").saveAsTable("simo_prod.db_alex.rsimprice")


Singtel_Price=f"""
select
  S.*,
  C.country_code
from
  simo_prod.db_alex.simo_singtel S
    left join (
      select distinct
        DESCRIPTION,
        country_code
      from
        simo_prod.ods.resource_res_mcc
    ) C
      on S.country = C.DESCRIPTION
"""

Rsim_Price=f"""
select 
  coalesce(Country,country_pccw) country,
  coalesce(carrier,Carrier_pccw) carrier,
  coalesce(country_code, country_code_pccw) Country_Code,
  PLMN,
  JT_price,
  JoyTel_Price,
  Date_Price PCCW_Price
from 
    simo_prod.db_alex.rsimprice
"""
Singtel_Price=spark.sql(Singtel_Price).toPandas()
display(Singtel_Price)
Rsim_Price=spark.sql(Rsim_Price).toPandas()
display(Rsim_Price)

import pandas as pd
from rapidfuzz import process, fuzz

def exclude(country, carrier_name):
    if pd.isna(carrier_name) or pd.isna(country):
        return carrier_name
    if country in carrier_name:
        carrier_name = carrier_name.replace(country, '').strip()
    return carrier_name

Singtel_Price['fixed_operator'] = Singtel_Price.apply(
    lambda row: exclude(row['Country'], row['Operator']),
    axis=1
)
Singtel_Price['fixed_operator'] = Singtel_Price.apply(
    lambda row: exclude(row['country_code'], row['fixed_operator']),
    axis=1
)

Rsim_Price['fixed_carrier']=Rsim_Price.apply(
    lambda row: exclude(row['country'], row['carrier']),
    axis=1
)

Rsim_Price['fixed_carrier']=Rsim_Price.apply(
    lambda row: exclude(row['Country_Code'], row['fixed_carrier']),
    axis=1
)

class CarrierMatcher:
    def __init__(self, left_df: pd.DataFrame, right_df: pd.DataFrame,
                 left_country_col: str, left_carrier_col: str,
                 right_country_col: str, right_carrier_col: str,
                 cutoff: int = 60):

        self.left_df = left_df.copy()
        self.right_df = right_df.copy()
        self.left_country_col = left_country_col
        self.left_carrier_col = left_carrier_col
        self.right_country_col = right_country_col
        self.right_carrier_col = right_carrier_col
        self.cutoff = cutoff
        self.result = None

    @staticmethod
    def fuzzy_match(x, choices, scorer=fuzz.ratio, cutoff=65):
        # Return best match string + score, or (None, None).
        match = process.extractOne(x, choices, scorer=scorer)
        if match and match[1] >= cutoff:
            return match[0], match[1]
        return (None, None)

    def run_matching(self):
        # Perform fuzzy matching between left_df and right_df
        best_matches, scores, matched_rows = [], [], []

        for _, row in self.right_df.iterrows():
            subset = self.left_df[self.left_df[self.left_country_col] == row[self.right_country_col]]

            if not subset.empty:
                best_carrier, score = self.fuzzy_match(
                    str(row[self.right_carrier_col]).lower(),
                    subset[self.left_carrier_col].astype(str).str.lower(),
                    cutoff=self.cutoff
                )
                if best_carrier:
                    matched_row = subset[subset[self.left_carrier_col].str.lower() == best_carrier]
                    if not matched_row.empty:
                        matched_rows.append(matched_row.iloc[0].to_dict())
                    else:
                        matched_rows.append({})
                else:
                    matched_rows.append({})
            else:
                best_carrier, score = (None, None)
                matched_rows.append({})

            best_matches.append(best_carrier)
            scores.append(score)

        # Attach results back
        self.right_df['best_match'] = best_matches
        self.right_df['match_score'] = scores

        # Merge side-by-side
        matched_df = pd.DataFrame(matched_rows)
        self.result = pd.concat([self.right_df.reset_index(drop=True),
                                 matched_df.reset_index(drop=True)], axis=1)
        return self.result


matcher = CarrierMatcher(
    left_df=Rsim_Price,
    right_df=Singtel_Price,
    left_country_col="Country_Code",
    left_carrier_col="fixed_carrier",
    right_country_col="country_code",
    right_carrier_col="fixed_operator"
)

final_result = matcher.run_matching()
final_result=final_result[['Country', 'Operator', '<250K (US$)\nAnnualized Revenue',
       '≥250K and <500K  (US$)\nAnnualized Revenue',
       '≥500K (US$)\nAnnualized Revenue', 'country_code','carrier']]

# final_result = final_result.drop(final_result.columns[-2], axis=1)

display(final_result)

merged = pd.merge(
    Rsim_Price,
    final_result, 
    how='outer',
    left_on=['Country_Code', 'carrier'],
    right_on=['country_code','carrier']
)

merged=merged.rename(columns={"Country":"Country_ST","country_code":"Country_Code_ST"})
import re

def clean_colname(col):
    # Replace invalid chars with underscore
    return re.sub(r'[^a-zA-Z0-9_]', '_', col).strip('_')

merged=merged.rename(columns={col: clean_colname(col) for col in merged.columns})
spark.createDataFrame(merged).write.option("mergeSchema", "true").mode("overwrite").saveAsTable("simo_prod.db_alex.rsim_summary_price")


syniverse_summary=f"""
select *from  simo_prod.db_alex.syniverse_summary
where carrier!='Not Found'
"""

rsim_summary=f"""
select 
  coalesce(country, country_st) Country,
  coalesce(carrier, operator) Carrier,
  coalesce(Country_Code, country_code_st) Country_Code,
  PLMN,
  JT_price,
  JoyTel_Price,
  PCCW_Price,
  250K__US___Annualized_Revenue as 250K__US___Annualized_Revenue_Singtel,
  250K_and__500K___US___Annualized_Revenue as 250K_and__500K___US___Annualized_Revenue_Singtel,
  500K__US___Annualized_Revenue as 500K__US___Annualized_Revenue_Singtel
from 
  simo_prod.db_alex.rsim_summary_price
order by coalesce(country, country_st) 
"""
syniverse_summary=spark.sql(syniverse_summary).toPandas()
rsim_summary=spark.sql(rsim_summary).toPandas()
display(syniverse_summary)
display(rsim_summary)

# Step 1: build lookup dictionary from rsim_summary
rsim_cols = [c for c in rsim_summary.columns if c != "PLMN"]
plmn_dict = rsim_summary.drop_duplicates(subset=['PLMN']).set_index("PLMN")[rsim_cols].to_dict(orient="index")

# Step 2: match function
def match_plmn_string(plmn_str):
    codes = [c.strip() for c in plmn_str.split(",")]
    for code in codes:
        if code in plmn_dict:
            row = plmn_dict[code].copy()
            row["PLMN_rsim"] = code  # keep matched PLMN
            return row
    # No match
    row = {col: None for col in rsim_cols}
    row["PLMN_rsim"] = None
    return row

# Step 3: apply to syniverse_summary
syniverse_summary = syniverse_summary.copy()  # avoid modifying original
syniverse_summary[rsim_cols + ["PLMN_rsim"]] = syniverse_summary["PLMN"].apply(
    lambda x: pd.Series(match_plmn_string(x))
)

# Step 4: add unmatched rsim_summary rows
all_syn_plmns = set(c.strip() for s in syniverse_summary["PLMN"] for c in s.split(","))
unmatched_rsim = rsim_summary[~rsim_summary["PLMN"].isin(all_syn_plmns)].copy()
for col in syniverse_summary.columns:
    if col not in ["PLMN"] + rsim_cols + ["PLMN_rsim"]:
        unmatched_rsim[col] = None
unmatched_rsim["PLMN_rsim"] = unmatched_rsim["PLMN"]  # keep original rsim PLMN

# Step 5: reorder columns
final_columns = list(dict.fromkeys(list(syniverse_summary.columns) + ["PLMN_rsim"]))  # ensure unique columns
unmatched_rsim = unmatched_rsim[final_columns]

# Step 6: combine
final_df = pd.concat([syniverse_summary, unmatched_rsim], ignore_index=True)

display(final_df)