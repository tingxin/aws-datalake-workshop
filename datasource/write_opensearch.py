from elasticsearch import Elasticsearch
import time
from mock import gen, DataType
import boto3
import requests
from requests_aws4auth import AWS4Auth
import json
# Create an Elasticsearch client
from datetime import datetime


region = 'ap-northeast-1'
service = 'es'
current_time = datetime.now().strftime('%Y-%m-%d-%H')
index_name = f'bie-hours-{current_time}'
url = f"https://search-bie-demo-sxc42bjy3ywvd5s3omc3w2udny.ap-northeast-1.es.amazonaws.com/{index_name}/_doc/"
credentials = boto3.Session().get_credentials()
print(credentials.access_key)
headers = { "Content-Type": "application/json" }

# Define the index and document data





focus_index = 'demo'
format_str = '%Y-%m-%d %H:%M:%S'

order_schema = {
    "user_mail": (DataType.Enum, ('barry.xu@163.com', 'dandan@qq.com', 'pony@qq.com', 'focus@qq.com')),
    "status": (DataType.Enum, ('unpaid', 'paid', 'cancel', 'shipping', 'finished')),
    "good_count": (DataType.INT, (1, 10)),
    "city":  (DataType.CITY,),
    "amount": (DataType.DOUBLE, (10, 1000)),
    "create_time": (DataType.DATETIME,),
    "update_time": (DataType.DATETIME,)
}

creator = gen(columns=order_schema, increment_id="sub_id",
              interval_min=100, interval_max=200)

for document in creator:
    document['timestamp'] = datetime.strptime(document['update_time'], format_str).timestamp()
    print(document)

    try:
         # Index the document
        response = requests.post(url, auth=('Demo', 'Demo@1234'), headers=headers, data=json.dumps(document))
        # Print the response
        print(response.json())

    except Exception as e:
        print(e)
        break
        time.sleep(60)

