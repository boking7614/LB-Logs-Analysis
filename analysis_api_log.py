# -*- coding: utf-8 -*-
import pandas as pd
from fnmatch import fnmatch
import gzip
import os
import time
import datetime
import json
import glob
from azure.storage.blob import BlockBlobService, PublicAccess
import requests
import re

# AWS LB logs download
def DownloadAwsAPILog(time):
    dt = time.split("-")
    os.system(f'aws s3 cp s3://iplaystarlogs/api-lb-logs/AWSLogs/204811385596/elasticloadbalancing/ap-northeast-2/{dt[0]}/{dt[1]}/{dt[2]}/ log/aws_api/ --quiet --recursive --exclude "*" --include "*{dt[0]}{dt[1]}{dt[2]}T{dt[3]}*" --profile s3')
    print(f'AWS API Log ({dt[0]}-{dt[1]}-{dt[2]}T{dt[3]}00) Download is done.')

# Azure LB logs download
def DownloadAzureAPILog(time):
    dt = time.split("-")
    block_blob_service = BlockBlobService(account_name='Account ID', account_key='Account Key')
    apidl = requests.get(block_blob_service.make_blob_url('insights-logs-applicationgatewayaccesslog', f'resourceId=/SUBSCRIPTIONS/3E1C6140-DDF3-45F1-8010-663A1216BDA9/RESOURCEGROUPS/PRODUCTION-EA-HK/PROVIDERS/MICROSOFT.NETWORK/APPLICATIONGATEWAYS/APPGW-API-CLARETFOX/y={dt[0]}/m={dt[1]}/d={dt[2]}/h={dt[3]}/m=00/PT1H.json'))
    open("log/azure_api/PT1H.json",'wb').write(apidl.content)
    print(f'PT1H.json({dt[0]}-{dt[1]}-{dt[2]}T{dt[3]}00) Download is done.')

# os.system('rm -f log/aws_api/* log/azure_api/*')

# time = input("Enter Time: ")

print(datetime.datetime.now(),"- Download API log")
# DownloadAzureAPILog(time)
# DownloadAwsAPILog(time)

host_ext_ids = []
host_info = []
aws_log_gzip = glob.glob("log/aws_api/204811385596*")

print(datetime.datetime.now(),"- Start Analysis API log")
# 分析AWS
for aws_log_file in aws_log_gzip:
    file_path = (aws_log_file)
    # with gzip.open(file_path, 'rb') as f:
    #     file_content = f.read()
    # with open(file_path.split('.log')[0], 'wb') as lf:
    #     lf.write(file_content)
    df=pd.read_csv(file_path.split('.log')[0], sep='\s+',header=None)
    for index, row in df.iterrows():
        if fnmatch(row[12], "*feed/gamehistory*") and fnmatch(row[12], "*&type=1*"):
            # print(f'{row[12]}[{row[26]}]')
            host_ext_ids.append((re.split('=|&', row[12]))[1])
            host_info.append((re.split('=|&', row[12]))[0] + "(" + (re.split('=|&', row[12]))[1] + ")")
             
# 分析AZURE
with open ("log/azure_api/PT1H.json", "r", encoding="utf-8") as jf:
    lines = jf.readlines()
    for line in lines:
        try:
            data = json.loads(line)
            if fnmatch(data['properties']['requestUri'], "*feed/gamehistory*") and fnmatch(data['properties']['requestQuery'], "*&type=1*"):
                # print((re.split('=|&', data['properties']['requestQuery']))[1])
                host_ext_ids.append((re.split('=|&', data['properties']['requestQuery']))[1])
                host_info.append(data['properties']['host'] + "(" + (re.split('=|&', data['properties']['requestQuery']))[1] + ")")
        except json.decoder.JSONDecodeError as e:
            print(e)
            
print(datetime.datetime.now(),"- Write to host_id.txt")
with open("host_id.txt", "a") as file:
    for host_ext_id in set(host_ext_ids):
        file.write(f"'{host_ext_id}',\n")

print(datetime.datetime.now(),"- Write to host_info.txt")
with open ("host_info.txt", "a") as hi:
    for i in set(host_info):
        hi.write(f'{i}\n')