import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as fn
from datetime import datetime, date, timedelta
import time
from secret import get_secret


# TODO 修改成您的 secret_name
mysql_secret_name = 'dev/demo/mysql'

ods_target_s3_path = 's3://lakehouse-practice1/order_ods/'
dwd_target_s3_path = 's3://lakehouse-practice1/order_ods/'

mysql_info = get_secret(mysql_secret_name)
mysql_jdbc = f"jdbc:mysql://{mysql_info['host']}:{mysql_info['port']}/demo"
mysql_user = mysql_info['username']
mysql_pass = mysql_info['password']


args = getResolvedOptions(sys.argv, ["JOB_NAME", 'mysqlJdbcS3path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# 从MySQL中读取数据
connection_mysql8_options = {
    "url": mysql_jdbc,
    "dbtable": "order_ex",
    "user": mysql_user,
    "password": mysql_pass,
    "customJdbcDriverS3Path": args['mysqlJdbcS3path'],
    "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver"
}

# # 如果表中有自增id,或者唯一值数值类递增字段， 可以使用如下方式进行增量同步
connection_mysql8_options['jobBookmarkKeys'] = ['sub_id']
connection_mysql8_options['jobBookmarkKeysSortOrder'] = 'asc'

# # 如果表中没有有增id,或者唯一值数值类递增字段， 可以使用如下方式进行增量同步
# now = datetime.now()
# today_begin = now.strftime("%Y-%m-%d 00:00:00")
# yesterday_begin = (now - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
# expression = f"create_time >= '{yesterday_begin}' and create_time < {today_begin}"
# connection_mysql8_options['hashexpression'] = expression
# connection_mysql8_options['hashpartitions'] = "10"

df_catalog = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options=connection_mysql8_options,
    # 如果要使用书签，这个上下文不能忽略
    transformation_ctx="df_catalog"
)


# use glue api
# df_filter = Filter.apply(frame = df_catalog, f = lambda x: x["amount"] >=10)
# use spark api
df = df_catalog.toDF()

# 创建日期分区键
df = df.withColumn("create_date", fn.to_date(df["create_time"]))

df.show(10)
print("========> {0}".format(df.count()))

# # example: ods 层数据 写入s3
# # TODO 将 s3://tx-glue-workshop/s3_dwd/ 替换成您的路径


dyn_ods_df = DynamicFrame.fromDF(df, glueContext, "nested")
glueContext.write_dynamic_frame.from_options(
    frame=dyn_ods_df,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": ods_target_s3_path,
        "partitionKeys": ["create_date", "user_mail"]
    }
)

# 添加你你的业务处理过程
# 例如：明细处理，过滤脏数据
df = df.filter(df["amount"] < 10000)
####################

# dwd 层数据
dyn_ods_df = DynamicFrame.fromDF(df, glueContext, "nested")
glueContext.write_dynamic_frame.from_options(
    frame=dyn_ods_df,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": dwd_target_s3_path,
        "partitionKeys": ["create_date", "user_mail"]
    }
)

job.commit()
