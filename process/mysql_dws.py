from multiprocessing import connection
import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as fn

s3_output = 's3://lakehouse-practice1/order_dws/'


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


df_order_dwd = glueContext.create_dynamic_frame.from_catalog(
    database="mall", table_name="order_dwd", transformation_ctx="df_order_dwd"
)

df_user_trace_dwd = glueContext.create_dynamic_frame.from_catalog(
    database="mall", table_name="user_trace", transformation_ctx="df_user_trace_dwd"
)


# use spark api
df_order = df_order_dwd.toDF()
df_user_trace = df_user_trace_dwd.toDF()

df_user_trace = df_user_trace.withColumn("email", df_user_trace["user_mail"])
df_user_trace = df_user_trace.drop(df_user_trace.user_mail)

cond = [df_order.user_mail == df_user_trace.email]
df_join = df_order.join(df_user_trace, on=cond, how="left")

df = df_join.groupBy(fn.col('user_mail')).agg(
    fn.count('order_id').alias('order_count'),
    fn.sum('good_count').alias('good_count'),
    fn.count('resource_id').alias('active_count'),
    fn.sum('amount').alias('amount')
)

df = df.select("user_mail", "order_count",
               "good_count", "amount", "active_count")
df.show(1)
# Script generated for node redshift_dws


# dws 层数据
dyn_dws_df = DynamicFrame.fromDF(df, glueContext, "nested")
glueContext.write_dynamic_frame.from_options(
    frame=dyn_dws_df,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": s3_output,
        "partitionKeys": ["user_mail"]
    }
)


job.commit()
