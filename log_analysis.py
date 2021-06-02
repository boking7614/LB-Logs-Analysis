# -*- coding: utf-8 -*-
import os
import json
import requests
import datetime
import time
import influxdb
from google.cloud import storage
from csv2json import convert, load_csv, save_json
from azure.storage.blob import BlockBlobService, PublicAccess

tstart = time.time()

now = datetime.datetime.now()
utc_time = datetime.datetime.today() - datetime.timedelta(hours=9)
y = utc_time.strftime('%Y')
m = utc_time.strftime('%m')
d = utc_time.strftime('%d')
h = utc_time.strftime('%H')

print("[*]Log檔下載中...")
# AZURE's Blob
block_blob_service = BlockBlobService(account_name='iplaystarlogs', account_key='AZURE儲存體驗證碼')
api_fn = ("api_log.json")
download_fn = ("dl_log.json")
apidl = requests.get(block_blob_service.make_blob_url('insights-logs-applicationgatewayaccesslog', "resourceId=/SUBSCRIPTIONS/3E1C6140-DDF3-45F1-8010-663A1216BDA9/RESOURCEGROUPS/PRODUCTION-EA-HK/PROVIDERS/MICROSOFT.NETWORK/APPLICATIONGATEWAYS/APPGW-API-CLARETFOX/y=" + y + "/m=" + m + "/d=" + d + "/h=" + h + "/m=00/PT1H.json"))
downloaddl = requests.get(block_blob_service.make_blob_url('insights-logs-applicationgatewayaccesslog', "resourceId=/SUBSCRIPTIONS/3E1C6140-DDF3-45F1-8010-663A1216BDA9/RESOURCEGROUPS/PRODUCTION-EA-HK/PROVIDERS/MICROSOFT.NETWORK/APPLICATIONGATEWAYS/APPGW-DOWNLOAD-CLARETFOX/y=" + y + "/m=" + m + "/d=" + d + "/h=" + h + "/m=00/PT1H.json"))
open(api_fn,'wb').write(apidl.content)
open(download_fn,'wb').write(downloaddl.content)

# GCP's bucket
storage_client = storage.Client.from_service_account_json('GCP Storage驗證Json檔路徑')
bucket_name = "ps_logs"
bucket = storage_client.get_bucket(bucket_name)

update_time = (y + "-" + m + "-" + d + "T" + h + ":50:00Z")
launch302_sum = 0
launch200_sum = 0
gethostgameinfo_sum = 0
az_resend_sum = 0
gcp_resend_sum = 0
resend_sum = az_resend_sum + gcp_resend_sum

print("[*]計算中...")
# AZURE應用程式閘道 API日誌 launch和gethostgameinfo次數計算
with open ("api_log.json", 'r') as jf:
    for line in jf.readlines():
        try:
            datas = json.loads(line)
            if line.find("launch") > 0 or line.find("Launch") > 0:
                if datas["properties"]["httpStatus"] == 302:
                    launch302_sum += 1
                elif datas["properties"]["httpStatus"] == 200:
                    launch200_sum += 1
            elif line.find("/host/gethostgameinfo") > 0:
                    gethostgameinfo_sum += 1
        except json.decoder.JSONDecodeError as e:
            print(now,"- ",e)

# AZURE應用程式閘道 Download 日誌 res_end.png次數計算
with open ("dl_log.json", 'r') as jf:
    for line in jf.readlines():
            if line.find("/res_end.png") > 0:
                az_resend_sum += 1

# GCP Download 日誌 res_end.png次數計算
blobs = bucket.list_blobs(prefix="dl_claretfox_usage_2019_05_" + y + "_" + m + "_" + d + "_" + h)
for blob in blobs:
    dl_blob = bucket.blob(blob.name)
    csv_path = ('logs/' + blob.name)
    json_path = ('logs/' + blob.name + '.json')
    blob.download_to_filename(csv_path)
    with open(csv_path) as r, open(json_path, 'w') as w:
        convert(r, w)
    with open (json_path, 'r') as jf:   
        datas = json.load(jf)
        for data in datas:
            if data["cs_uri"] == ("/res_end.png"):
                gcp_resend_sum +=1


json_body = json.loads('[{"measurement": "newlog", "time": "' + update_time  + '","fields": {"launch200": ' + str(launch200_sum) + ', "launch302": '  + str(launch302_sum) + ', "gethostgameinfo": ' + str(gethostgameinfo_sum) + ',"resend": ' + str(az_resend_sum + gcp_resend_sum) + '}}]')

print("[*]上傳資料中...")
db = influxdb.InfluxDBClient('127.0.0.1', 8086, 'log', 'playstar123!@#', 'cloudlogs')
db.write_points(json_body)

os.system("rm -f logs/*")

tend = time.time()
print("[*]Done(%fs)"%(tend - tstart))
print("================================")
print("launch200：" ,launch200_sum)
print("launch302：" ,launch302_sum)
print("gethostgameinfo：" ,gethostgameinfo_sum)
print("res_end.png：" ,az_resend_sum + gcp_resend_sum)
print("================================")
