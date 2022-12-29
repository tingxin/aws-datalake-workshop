import sys
import requests
import boto3
import json

# 临时表的s3地址
target_s3 = "s3://example-output/zailab/user_job_info/"
# athena 中，你的数据表放在那个db 下，那个目录下。在athena web 界面能看到
athena_data_ctx = {'Database': 'default', 'Catalog': 'AwsDataCatalog'}

# athena 的 log地址
athena_output_cfg = {
    'OutputLocation': 's3://aws-glue-assets-027040934161-cn-northwest-1/'}

# 目标表表名
target_table = "demo_zailab_user_job_tb"

if __name__ == "__main__":
    resp = requests.get("http://172.31.43.238:5016")
    body = resp.text
    bytes_data = body.encode()
    s3 = boto3.client("s3")
    response = s3.put_object(
        Body=bytes_data,
        Bucket='example-output',
        Key='zailab/user_job_info/data.json',
    )
    print(response)
