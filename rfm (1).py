#!/usr/bin/env python
# coding: utf-8
print('here')
import sys
import os
import json
import numpy as np
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import dateutil
from tqdm import tqdm
import requests

file_dir = os.path.dirname(__file__)
base_dir = os.path.dirname(file_dir)

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = base_dir + "/core/.mg_in_creds.json"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/myglamm/zegulas/myglamm/pem/maclynn_ds_server.json"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/myglamm/zegulas/zegulas/myglamm/pem/maclynn_ds_server.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/centos/data_transform/core/.mg_in_creds.json"

yesterday = datetime.now() - timedelta(1)  

# tilldate = sys.argv[1]
tilldate = datetime.strftime(yesterday, '%Y-%m-%d')
# tilldate = '2021-12-22'

client=bigquery.Client()

# # query_1 = "select m._id as member_id, o.consumer_id as consumer_id, sum(payable_amount) as total_payable_amount,count(o.id) as order_count, DATE_DIFF('" + tilldate + "', parse_date('%Y-%m-%d',max(order_date_ist)), day) as diff_in_date from `myglamm-india.master_tables.master_order_table` o inner join `myglamm-india.master_tables.master_member_table` m on o.member_id = m._id where o.status_id = 15 and m.type = 1 and m.is_member_bg = 0 and o.member_id not in ('5a1d23b5c0b04949c988a5c1','59f8730fb97262240ac62a99') and date_created_ist <= '" + tilldate + "' and order_date_ist <= '" + tilldate + "' group by member_id, consumer_id"

# # query_1 = "select m._id as member_id, sum(payable_amount) as total_payable_amount,count(o.id) as order_count, DATE_DIFF('" + tilldate + "', parse_date('%Y-%m-%d',max(order_date_ist)), day) as diff_in_date from `myglamm-india.master_tables.master_order_table_v2` o inner join `myglamm-india.master_tables.master_member_table_v2` m on o.member_id = m._id where o.status_id in (15) and m.type != 'retailer' and m.is_member_bg = 0 and o.member_id not in ('5a1d23b5c0b04949c988a5c1','59f8730fb97262240ac62a99') and date_created_ist <= '" + tilldate + "' and order_date_ist <= '" + tilldate + "' group by member_id"

query_1 = """
    select
        m._id as member_id,
        case
            when total_payable_amount is null then 0
            else total_payable_amount
        end as total_payable_amount,
        case
            when order_count is null then 0
            else order_count
        end as order_count,
        case
            when diff_in_date is null then 0
            else diff_in_date
        end as diff_in_date,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(),date_created_ist,day) <= 60 THEN 'RNP_60'
            WHEN DATE_DIFF(CURRENT_DATE(),date_created_ist,day) between 61 and 120 THEN 'RNP_120'
            ELSE 'RNP_120+'
        END as category,
        '000' as rfm_score,
        '""" + tilldate + """' as created_on
    from
        `myglamm-india.master_tables.master_member_table_v2` m
    left outer join (
        select
            m._id as member_id,
            sum(ifnull(payable_amount,0)) as total_payable_amount,
            count(distinct o.id) as order_count,
            DATE_DIFF('""" + tilldate + """', parse_date('%Y-%m-%d',max(order_date_ist)), day) as diff_in_date
        from
            `myglamm-india.master_tables.master_order_table_v2` o
        inner join
            `myglamm-india.master_tables.master_member_table_v2` m on o.member_id = m._id
        where
            o.order_type in (0,2)
            and o.status_id in (12,14,15,20,74,75,11)
            and m.type != 'retailer'
            and m.is_member_bg = 0
            and o.member_id not in ('5a1d23b5c0b04949c988a5c1','59f8730fb97262240ac62a99')
            and date_created_ist <= '""" + tilldate + """'
            and order_date_ist <= '""" + tilldate + """'
        group by
            member_id
    ) o on m._id = o.member_id
    order by member_id asc
-- limit 5000000 offset 0"""

# 3915922
# print(query_1)

#postive status ids 12,14,15,20,74,75,11

client=bigquery.Client()

# print(client)

df1=client.query(query_1).to_dataframe()

# print(df1.head())

df=df1[(df1['order_count']>0)]
# print(df.head())
dfr=df1[(df1['order_count'] == 0)]
# print(dfr.head())
dfr1=dfr[['member_id', 'category','rfm_score','created_on']]
# print(dfr1.head())

#df = pd.read_csv('/tmp/rfm.csv')
df=df.rename(columns={'total_payable_amount':'revenue', 'order_count':'frequency', 'diff_in_date':'recency'})
# print(df.head())
#df=df.set_index('member_id')

quartiles = df.quantile(q=[0.2,0.4,0.6,0.8])
quartiles = quartiles.to_dict()

def RClass(x,p,d):
    # if x == 0:
    #     return 0
    # el
    if x <= d[p][0.2]:
        return 1
    elif x <= d[p][0.4]:
        return 2
    elif x <= d[p][0.6]: 
        return 3
    elif x <= d[p][0.8]: 
        return 4
    else:
        return 5

## function for Frequency and Monetary value score

def FMClass(x,p,d):
    # if x == 0:
    #     return 0
    # el
    if x <= d[p][0.2]:
        return 5
    elif x <= d[p][0.4]:
        return 4
    elif x <= d[p][0.6]: 
        return 3
    elif x <= d[p][0.8]: 
        return 2
    else:
        return 1  

rfmSeg = df
rfmSeg['R_Quartile'] = rfmSeg['recency'].apply(RClass, args=('recency',quartiles,))
rfmSeg['F_Quartile'] = rfmSeg['frequency'].apply(FMClass, args=('frequency',quartiles,))
rfmSeg['M_Quartile'] = rfmSeg['revenue'].apply(FMClass, args=('revenue',quartiles,))

rfmSeg['RFMClass'] = rfmSeg.R_Quartile.map(str) + rfmSeg.F_Quartile.map(str) + rfmSeg.M_Quartile.map(str)

rfmSeg['Total Score'] = rfmSeg['R_Quartile'].astype(str) + rfmSeg['F_Quartile'].astype(str) + rfmSeg['M_Quartile'].astype(str)
rfmSeg['rfm_score'] = rfmSeg['R_Quartile'].astype(str) + rfmSeg['F_Quartile'].astype(str) + rfmSeg['M_Quartile'].astype(str)

# print('len(rfmSeg)')
# print(len(rfmSeg))

c_list= ['111', '112', '121', '122', '131', '132', '211', '212', '221', '222', '231', '221', '222', '231']
h_list= ['453', '454', '455', '524', '525', '551', '552', '553', '554', '555', '552', '553', '554', '555']
p_list= ['151', '152', '153', '154', '155', '251', '252', '254', '255', '155', '251', '252', '254', '255']
na_list=['352', '353', '354', '355', '414', '415', '424', '425', '452', '514', '515', '452', '514', '515']
cl_list=['351', '411', '412', '413', '421', '422', '423', '451', '511', '512', '513', '521', '522', '523']
l_lit=  ['113', '123', '213', '223', '311', '312', '313', '321', '322', '323', '313', '321', '322', '323']
pl_list=['114', '115', '124', '125', '214', '215', '224', '225', '253', '314', '315', '324', '325', '325']

for i,j,k,l,m,n,o in zip(c_list, h_list, p_list, na_list, cl_list, l_lit, pl_list):
    rfmSeg.loc[rfmSeg['RFMClass']==i,"category"]="Champions"
    rfmSeg.loc[rfmSeg['RFMClass']==j,"category"]="Hibernating"
    rfmSeg.loc[rfmSeg['RFMClass']==k,"category"]="Promising"
    rfmSeg.loc[rfmSeg['RFMClass']==l,"category"]="Needs Attention"
    rfmSeg.loc[rfmSeg['RFMClass']==m,"category"]="Can not loose"
    rfmSeg.loc[rfmSeg['RFMClass']==n,"category"]="Loyalist"
    rfmSeg.loc[rfmSeg['RFMClass']==o,"category"]="Potential Loyalist"

time_2 = datetime.now()

# print('after for loop')

# print(rfmSeg.head())

#rfmSeg['created_on'] = tilldate

rfmSeg = rfmSeg[['member_id', 'category', 'rfm_score', 'created_on']]
rfmSeg = rfmSeg.append(dfr1)
#'consumer_id',
#rfmSeg.to_json('rfm.json', orient='records', lines=True)
# print(rfmSeg)

# print('creating csv')
rfmSeg.to_csv('rfm.csv',index=False)

print('done creating csv..')


# TODO(developer): Set table_id to the ID of the table to create.
table_id = "myglamm-india.data_science.member_rfm"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("member_id", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("rfm_score", "STRING"),
        bigquery.SchemaField("created_on", "STRING"),
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
# uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
file_path = "rfm.csv"

with open(file_path, "rb") as source_file:
    load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))

# before we de-dup the table, it is necessary to push the 
# new records / updated records into an append only table which will be called
# member_rfm_log, which will keep a track of how the category of member evolved over time.

log_append_query = '''
INSERT `myglamm-india.data_science.member_rfm_log` (member_id, category, rfm_score, created_on) 
select distinct 
    member_id, 
    category, 
    rfm_score, 
    created_on
from (
    select 
        t1.member_id, 
        t1.category, 
        t1.rfm_score, 
        t1.created_on
    from 
    (
        select * from (
            select 
                *,
                row_number() over (partition by member_id order by created_on desc) as rank
            FROM 
                `myglamm-india.data_science.member_rfm`
        ) where rank = 1
    ) t1 join (
        select * from (
            select 
                *,
                row_number() over (partition by member_id order by created_on desc) as rank
            FROM 
                `myglamm-india.data_science.member_rfm`
        ) where rank = 2
    ) t2 on t1.member_id=t2.member_id
    where t1.category!=t2.category

    union all

    select 
        t1.member_id, 
        t1.category, 
        t1.rfm_score, 
        t1.created_on
    from 
    (
        select * from (
            select 
                *,
                row_number() over (partition by member_id order by created_on desc) as rank
            FROM 
                `myglamm-india.data_science.member_rfm`
        ) where rank = 1
    ) t1 left join (
        select * from (
            select 
                *,
                row_number() over (partition by member_id order by created_on desc) as rank
            FROM 
                `myglamm-india.data_science.member_rfm`
        ) where rank = 2
    ) t2 on t1.member_id=t2.member_id
    where t1.member_id is not null and t2.member_id is null

)

''' 


# query_1 = """
# select 
#     t1.member_id, 
#     t1.category, 
#     t1.rfm_score, 
#     t1.created_on
# from 
# (
#     select * from (
#         select 
#             *,
#             row_number() over (partition by member_id order by created_on desc) as rank
#         FROM 
#             `myglamm-india.data_science.member_rfm`
#     ) where rank = 1
# ) t1 join (
#     select * from (
#         select 
#             *,
#             row_number() over (partition by member_id order by created_on desc) as rank
#         FROM 
#             `myglamm-india.data_science.member_rfm`
#     ) where rank = 2
# ) t2 on t1.member_id=t2.member_id
# where t1.category!=t2.category 
# --or (t2.member_id is null and t1.created_on='""" + tilldate + """')
# -- and t1.member_id='61bcb72e3e70ab001b2c9018'

# union all

# select 
#     t1.member_id, 
#     t1.category, 
#     t1.rfm_score, 
#     t1.created_on
# from 
# (
#     select * from (
#         select 
#             *,
#             row_number() over (partition by member_id order by created_on desc) as rank
#         FROM 
#             `myglamm-india.data_science.member_rfm`
#     ) where rank = 1
# ) t1 left join (
#     select * from (
#         select 
#             *,
#             row_number() over (partition by member_id order by created_on desc) as rank
#         FROM 
#             `myglamm-india.data_science.member_rfm`
#     ) where rank = 2
# ) t2 on t1.member_id=t2.member_id
# where t1.member_id is not null and t2.member_id is null
# """

log_append_query_job=client.query(log_append_query)

log_append_results = log_append_query_job.result()

#for row in log_append_results:
    #print(row)

# de-duplicate table query to be run after new set of rfm data calculated members are updated in member_rfm table.
dedup_query = '''
create or replace table `myglamm-india.data_science.member_rfm` as
select * except(rank) from (
SELECT 
  *,
  row_number() over (partition by member_id order by created_on desc) as rank
FROM `myglamm-india.data_science.member_rfm`
) where rank = 1
'''


dedup_query=client.query(dedup_query)

dedup_results = dedup_query.result()

#for row in dedup_results:
    #print(row)

# webengage sync the delta.


# webengage_sync_rfm = """
# select 
#     t1.member_id,
#     t1.category as new_rfm
# from 
# (
#     select * from (
#         select 
#             *,
#             row_number() over (partition by member_id order by created_on desc) as rank
#         FROM 
#             `myglamm-india.data_science.member_rfm`
#     ) where rank = 1
# ) t1 left join (
#     select * from (
#         select 
#             *,
#             row_number() over (partition by member_id order by created_on desc) as rank
#         FROM 
#             `myglamm-india.data_science.member_rfm`
#     ) where rank = 2
# ) t2 on t1.member_id=t2.member_id
# where t1.category!=t2.category
# -- and t1.member_id>'60bb2edfb7ff79001a2d86f0'
# and t1.created_on = '""" + tilldate + """'
# """

webengage_sync_rfm = """
select 
    t1.member_id,
    t1.category as new_rfm
from `myglamm-india.data_science.member_rfm_log` t1
where t1.created_on = '""" + tilldate + """'
"""

# 3915922
#print(webengage_sync_rfm)

#postive status ids 12,14,15,20,74,75,11

webengage_sync_rfm_query_job=client.query(webengage_sync_rfm)

webengage_sync_rfm_results = webengage_sync_rfm_query_job.result()
url = "https://acl.mgapis.com/dump-ms/dump"
headers = {
  'accept': '*/*',
  'Content-Type': 'application/json',
  'apiKey': 'fac2c144a7da65d81c939597bf983613'
}

count = 0
counter = 0
temp_arr = []
for row in webengage_sync_rfm_results:
    #print(row)
    #print(row.member_id)
    #print(row.new_rfm)
    member_id = row.member_id
    new_rfm = row.new_rfm
    # df1=client.query(query_1).to_dataframe()

    # print(df1.head())

    count = count+1
    print(count)

    if(counter<10):
        temp_arr.append({
            "vendorCode": "mgp",
            "identifier": member_id,
            "key": "webengageSync",
            "value": {
                "userId": member_id,
                "attributes": {
                    "RFM Category": new_rfm
                }
            }
        })
        counter = counter + 1

    if(counter==10):
        counter = 0
        payload = json.dumps(
            temp_arr
        )
        temp_arr = []

        #print(payload)
        response = requests.request("POST", url, headers=headers, data=payload)

        #print(response.text)

'''

curl --location --request POST 'https://acl.mgapis.com/dump-ms/dump' \
--header 'Content-Type: application/json' \
--header 'apiKey: fac2c144a7da65d81c939597bf983613' \
--data-raw '[
    {
        "vendorCode": "mgp",
        "identifier": "59b27ca219141863361aa408",
        "key": "webengageSync",
        "value": {
            "userId": "59b27ca219141863361aa408",
            "attributes": {
                "RFM Category": "Promising"
            }
        }
    }
]'

'''

# add deadmansnitch alert

requests.request("GET","https://nosnch.in/5d97244a65")

print('done')    


