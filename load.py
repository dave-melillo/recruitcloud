#!/usr/bin/env python
# coding: utf-8

# In[17]:


#https://medium.com/@thabo_65610/three-ways-to-automate-python-via-jupyter-notebook-d14aaa78de9
#import modules
from google.cloud import bigquery
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import requests
from pandas.io.json import json_normalize 
import numpy as np
import matplotlib
from sqlalchemy.types import Integer, Text, String, DateTime
import pandas_gbq as pd_gbq
from IPython.display import display, HTML
from flatten_json import flatten
import datetime as dt


# In[18]:


#function to create one dataframe from list of dataframes

def union_list_of_dfs(df_list):

  if len(df_list) > 0:

        try:

            df = df_list[0].append(df_list[1:])

        except Exception as e:

            df = df_list[0]

            if len(df_list) == 1:

                print(f'only single chunk returned')

            else:

                print(f'{sys.exc_info()[0]}- {str(e)} - at line: {sys.exc_info()[2].tb_lineno}')

#         print(f'df of length {len(df)} successfully created')

        return df

  else:

        print('no data returned')


# In[19]:


#recruiter flow
#loop and store

#create empty dataframe
raw_data = pd.DataFrame([])
table_name = 'raw.rf_candidate_prod'
project_id = 'rcdatawarehouse'

#initialize loop
for i in range(1, 1000):
    if len(requests.get("https://recruiterflow.com/api/external/candidate/list?items_per_page=100&current_page="+str(i), headers={"rf-api-key":"XXX"}).json()) == 0:
        break
    else:
        res=requests.get("https://recruiterflow.com/api/external/candidate/list?items_per_page=100&current_page="+str(i), headers={"rf-api-key":"XXX"}).json()
        data = json_normalize(res)
        df_temp = pd.DataFrame.from_dict(data)
        raw_data = raw_data.append(df_temp,ignore_index=True,sort=False)
        print("Finished loading page " + str(i))

raw_data.columns = raw_data.columns.str.replace(".", "_")

raw_data = raw_data.astype(str)

pd_gbq.to_gbq(raw_data, table_name, project_id, if_exists='replace')

print("All done!")




#contacts

import os
import boto3
import pandas as pd
import sys

if sys.version_info[0] < 3: 
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x


client = boto3.client('s3', aws_access_key_id='XXX',
        aws_secret_access_key='XXX')

bucket_name = 'rcdatawarehouse'

object_key = 'dumps/contacts/full.csv'
csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

df = pd.read_csv(StringIO(csv_string))

df = df.astype(str)

pd_gbq.to_gbq(df, 'raw.sw_contacts_prod', project_id='rcdatawarehouse', if_exists='replace')

print('You just wrote ' + str(len(df)) + ' rows to the sw_contacts table')


# In[8]:


#opens

import os
import boto3
import pandas as pd
import sys

if sys.version_info[0] < 3: 
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

df_list = []

s3_client = boto3.client('s3', aws_access_key_id='XXX',
        aws_secret_access_key='XXX')

paginator = s3_client.get_paginator('list_objects_v2')

res = paginator.paginate(Bucket='rcdatawarehouse',Prefix='dumps/')

for p in res:
    if "Contents" in p:
        for key in p["Contents"]:
            if 'opens' in key["Key"].lower():
                csv_obj = s3_client.get_object(Bucket='rcdatawarehouse', Key=key["Key"])
                body = csv_obj['Body']
                csv_string = body.read().decode('utf-8')
                df = pd.read_csv(StringIO(csv_string))
                df2 = df2 = df
                df_list.append(df2)

open_df = union_list_of_dfs(df_list)

open_df = open_df.astype(str)

pd_gbq.to_gbq(open_df, 'raw.sw_opens_prod', project_id='rcdatawarehouse', if_exists='replace')

print('You just wrote ' + str(len(open_df)) + ' rows to the opens table')


# In[10]:


#clicks

import os
import boto3
import pandas as pd
import sys

if sys.version_info[0] < 3: 
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

df_list = []

s3_client = boto3.client('s3', aws_access_key_id='AKIARDGZKJ3H3PPIS6X4',
        aws_secret_access_key='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')

paginator = s3_client.get_paginator('list_objects_v2')

res = paginator.paginate(Bucket='rcdatawarehouse',Prefix='dumps')

for p in res:
    if "Contents" in p:
        for key in p["Contents"]:
            if 'clicks' in key["Key"].lower():
                try: 
                    csv_obj = s3_client.get_object(Bucket='rcdatawarehouse', Key=key["Key"])
                    body = csv_obj['Body']
                    csv_string = body.read().decode('utf-8')
                    df = pd.read_csv(StringIO(csv_string))
                    df2 = df2 = df
                    df_list.append(df2)
                except pd.errors.EmptyDataError:
                    df = pd.DataFrame()

clicks_df = union_list_of_dfs(df_list)

clicks_df = clicks_df.astype(str)

pd_gbq.to_gbq(clicks_df, 'raw.sw_clicks_prod', project_id='rcdatawarehouse', if_exists='replace')

print('You just wrote ' + str(len(clicks_df)) + ' rows to the clicks table')


# In[11]:


#replies

import os
import boto3
import pandas as pd
import sys

if sys.version_info[0] < 3: 
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

df_list = []

s3_client = boto3.client('s3', aws_access_key_id='XXX',
        aws_secret_access_key='XXX')

paginator = s3_client.get_paginator('list_objects_v2')

res = paginator.paginate(Bucket='rcdatawarehouse',Prefix='dumps/')

for p in res:
    if "Contents" in p:
        for key in p["Contents"]:
            if 'replies' in key["Key"].lower():
                try: 
                    csv_obj = s3_client.get_object(Bucket='rcdatawarehouse', Key=key["Key"])
                    body = csv_obj['Body']
                    csv_string = body.read().decode('utf-8')
                    df = pd.read_csv(StringIO(csv_string))
                    df2 = df2 = df
                    df_list.append(df2)
                except pd.errors.EmptyDataError:
                    df = pd.DataFrame()

replies_df = union_list_of_dfs(df_list)

replies_df.rename(columns={'message-id':'messageid'}, inplace=True)

pd_gbq.to_gbq(replies_df, 'raw.sw_replies_prod', project_id='rcdatawarehouse', if_exists='replace')

print('You just wrote ' + str(len(replies_df)) + ' rows to the replies table')


# In[12]:


#sent-mail

import os
import boto3
import pandas as pd
import sys

if sys.version_info[0] < 3: 
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

df_list = []

s3_client = boto3.client('s3', aws_access_key_id='XXX',
        aws_secret_access_key='XXX')

paginator = s3_client.get_paginator('list_objects_v2')

res = paginator.paginate(Bucket='rcdatawarehouse',Prefix='dumps/')

for p in res:
    if "Contents" in p:
        for key in p["Contents"]:
            if 'mail' in key["Key"].lower():
                try: 
                    csv_obj = s3_client.get_object(Bucket='rcdatawarehouse', Key=key["Key"])
                    body = csv_obj['Body']
                    csv_string = body.read().decode('utf-8')
                    df = pd.read_csv(StringIO(csv_string))
                    df2 = df2 = df
                    df_list.append(df2)
                except pd.errors.EmptyDataError:
                    df = pd.DataFrame()
            

sm_df = union_list_of_dfs(df_list)

sm_df.rename(columns={'message-id':'messageid'}, inplace=True)

pd_gbq.to_gbq(sm_df, 'raw.sw_sent_mail_prod', project_id='rcdatawarehouse', if_exists='replace')

print('You just wrote ' + str(len(sm_df)) + ' rows to the sent_mail table')


# In[ ]:




