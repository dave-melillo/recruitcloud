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

#recruiter flow
#loop and store

#create empty dataframe
raw_data = pd.DataFrame([])
table_name = 'raw.rf_candidate2'
project_id = 'rcdatawarehouse'

#initialize loop
for i in range(1, 1000):
    if len(requests.get("https://recruiterflow.com/api/external/candidate/list?items_per_page=100&current_page="+str(i), headers={"rf-api-key":"XXXXXX"}).json()) == 0:
        break
    else:
        res=requests.get("https://recruiterflow.com/api/external/candidate/list?items_per_page=100&current_page="+str(i), headers={"rf-api-key":"XXXXXX"}).json()
        data = json_normalize(res)
        df_temp = pd.DataFrame.from_dict(data)
        raw_data = raw_data.append(df_temp,ignore_index=True,sort=False)
        print("Finished loading page " + str(i))

raw_data.columns = raw_data.columns.str.replace(".", "_")

pd_gbq.to_gbq(raw_data, table_name, project_id=project_id, if_exists='replace')

print("All done!")

import os
import boto3
import pandas as pd
import sys

if sys.version_info[0] < 3: 
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x


client = boto3.client('s3', aws_access_key_id='XXXXXXXXXXX',
        aws_secret_access_key='XXXXXXXXXXXX')

bucket_name = 'XXXXXXXXX'

object_key = 'XXXXXXXX.csv'
csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

df = pd.read_csv(StringIO(csv_string))

pd_gbq.to_gbq(df, 'raw.sw_contacts1', project_id='XXXXXX', if_exists='replace')

print("All done!")
