import sys
from faker import Faker
from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext
from random import random
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, \
    IntegerType, TimestampType, DoubleType, LongType

import json


@udf(returnType=StringType())
def extract_partition_date(json_str):
    t = json.loads(json_str)
    views = t['views']
    new_views = [{
        'id': view['id'],
        'name': view['name'],
        'type': view['type']
    } for view in views]
    return json.dumps({
        "views": new_views
    })


def run():
    spark = SparkSession.builder.getOrCreate()

    columns = ["key", "jsonstr"]
    vals = [
        ("abc", """
        {"views": [{"id": "viwKvnA5xK8kU", "name": "维格视图", "rows": [{"recordId": "reckFWfJKUN2u"}, {"recordId": "recjTb1flUSEg"}, {"recordId": "rectjAJwqK72G"}, {"recordId": "recAStdM7pTeY"}, {"recordId": "recxWVFj8Djo8"}, {"recordId": "recMQ6qducLwA"}, {"recordId": "recXowcynmFqM"}, {"recordId": "rec75uZBrtJaG"}, {"recordId": "recgNYgZJeEKM"}, {"recordId": "recrBRVUWFfPg"}], "type": 1, "columns": [{"fieldId": "fldd8TUVXaac2"}, {"fieldId": "fldg196TPXJhM"}, {"fieldId": "fld219eydN7Pf"}, {"width": 400, "fieldId": "fld8g8sZjYzLZ"}, {"fieldId": "fldAmBwSCUXNY"}, {"fieldId": "fldqYqc3HCZZu"}, {"fieldId": "fldkxH9NxCS9r"}], "frozenColumnCount": 1}, {"id": "viwBENUXgZ3PG", "name": "相册视图", "rows": [{"recordId": "reckFWfJKUN2u"}, {"recordId": "recjTb1flUSEg"}, {"recordId": "rectjAJwqK72G"}, {"recordId": "recAStdM7pTeY"}, {"recordId": "recxWVFj8Djo8"}, {"recordId": "recMQ6qducLwA"}, {"recordId": "recXowcynmFqM"}, {"recordId": "rec75uZBrtJaG"}, {"recordId": "recgNYgZJeEKM"}, {"recordId": "recrBRVUWFfPg"}], "type": 3, "style": {"cardCount": 4, "isCoverFit": false, "layoutType": 0, "isAutoLayout": false}, "columns": [{"fieldId": "fldd8TUVXaac2"}, {"fieldId": "fldg196TPXJhM"}, {"fieldId": "fld219eydN7Pf"}, {"fieldId": "fld8g8sZjYzLZ"}, {"fieldId": "fldAmBwSCUXNY"}, {"hidden": true, "fieldId": "fldqYqc3HCZZu"}, {"hidden": true, "fieldId": "fldkxH9NxCS9r"}]}], "fieldMap": {"fld219eydN7Pf": {"id": "fld219eydN7Pf", "name": "交通", "type": 1, "description": ""}, "fld8g8sZjYzLZ": {"id": "fld8g8sZjYzLZ", "name": "景点", "type": 1, "description": ""}, "fldAmBwSCUXNY": {"id": "fldAmBwSCUXNY", "name": "住宿", "type": 3, "property": {"options": [{"id": "opt10101daban", "name": "大阪难波奏酒店", "color": 4}, {"id": "opt0202jingdu", "name": "京都格兰比亚酒店", "color": 5}, {"id": "opt03xianggen", "name": "箱根山景酒店", "color": 6}, {"id": "opt04dongjing", "name": "东京帕克酒店", "color": 7}]}, "description": ""}, "fldd8TUVXaac2": {"id": "fldd8TUVXaac2", "name": "时间", "type": 5, "property": {"autoFill": false, "dateFormat": "yyyy-MM-dd", "timeFormat": ""}, "description": ""}, "fldg196TPXJhM": {"id": "fldg196TPXJhM", "name": "城市", "type": 4, "property": {"options": [{"id": "opt1guangzhou", "name": "广州", "color": 0}, {"id": "opt02dongjing", "name": "东京", "color": 1}, {"id": "opt30303daban", "name": "大阪", "color": 2}, {"id": "opt0404jingdu", "name": "京都", "color": 3}, {"id": "opt05nailiang", "name": "奈良", "color": 4}, {"id": "opt06xianggen", "name": "箱根", "color": 5}]}, "description": ""}, "fldkxH9NxCS9r": {"id": "fldkxH9NxCS9r", "name": "摄影", "type": 6, "description": ""}, "fldqYqc3HCZZu": {"id": "fldqYqc3HCZZu", "name": "花费", "type": 2, "property": {"precision": 1}, "description": ""}}}
        """)
    ]
    df = spark.createDataFrame(vals, columns)
    df = df.withColumn("views", extract_partition_date(df["jsonstr"]))
    df = df.drop('jsonstr')
    df.printSchema()
    df.write.json(
        "/home/ec2-user/workshop/aws-datalake-workshop/temp/hem.json")


run()
