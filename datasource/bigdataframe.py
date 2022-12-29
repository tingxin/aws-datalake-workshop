import sys
from faker import Faker
from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


args = getResolvedOptions(
    sys.argv, ['tablename', 's3outpath', 'partition', 'JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


total_rows = 50000000
unit_row = 200000

# total_rows = 50
# unit_row = 10

faker = Faker()


def facker_bool():
    return faker.random_int(0, 1) == 1


def facker_int():
    return faker.random_int(0, 100000)


def flt_trackingnumber():
    return str(faker.random_int(0, 10000000))


flt_columns_checker = {
    'trackingnumber': flt_trackingnumber
}


def get_meta(create_table_sql: str, spec_columns_checker: dict = None):
    columns_begin = create_table_sql.find('(')
    columns_end = create_table_sql.rfind(")")
    columns_str = create_table_sql[columns_begin+1:columns_end-1]

    columns_parts = columns_str.split(',\n')
    print(columns_parts)
    columns_parts = [item.strip() for item in columns_parts]

    columns = list()
    columns_value_funcs = list()
    for part in columns_parts:
        if part.startswith("PRIMARY"):
            continue
        print(part)
        kv = part.split(' ')
        k = kv[0]
        v = kv[1]
        if k.startswith('"'):
            k = k[1:len(k)-1]
        columns.append(k)

        v = v.lower()
        if spec_columns_checker and k in spec_columns_checker:
            f = spec_columns_checker[k]
        elif v.startswith("varchar"):
            f = faker.pystr
        elif v.startswith("timestamp"):
            f = faker.date_this_year
        elif v.startswith("numeric") or v.startswith("int") or v.startswith("bigint"):
            f = facker_int
        elif v.startswith("bool"):
            f = facker_bool
        else:
            f = faker.pystr

        columns_value_funcs.append(f)

    return columns, columns_value_funcs


def get_oms_parcels():
    sql = """
    CREATE TABLE "dk"."oms_parcels" (
  "id" varchar(255) NOT NULL,
  "createdby" varchar(255),
  "createdat" timestamp,
  "updatedat" timestamp,
  "updatedby" varchar(255),
  "account" varchar(255),
  "batch" varchar(255),
  "client" varchar(255),
  "command" varchar(255),
  "container" varchar(255),
  "items" varchar(65535),
  "trackingnumber" varchar(255),
  "transporter" varchar(255),
  "weight" numeric(19,2) NOT NULL,
  "zipcode" varchar(255),
  "ld3" varchar(255),
  "destination_code" varchar(255),
  PRIMARY KEY("id", "trackingnumber")
)
    """
    return get_meta(sql,  flt_columns_checker)


def get_oms_shipments():
    sql = """
    CREATE TABLE "dk"."oms_shipments" (
  "id" varchar(255) NOT NULL,
  "createdat" timestamp,
  "createdby" varchar(255),
  "updatedat" timestamp,
  "updatedby" varchar(255),
  "client" varchar(255) NOT NULL,
  "reference" varchar(255),
  "description" varchar(255),
  "price" numeric(28,6),
  "recipientcity" varchar(255),
  "recipientcountry" varchar(255),
  "recipientname" varchar(255),
  "recipientphonenumber" varchar(255),
  "recipientzipcode" varchar(255),
  "checkedat" timestamp,
  "status" varchar(255),
  "trackingnumber" varchar(255),
  "transporter" varchar(255) NOT NULL,
  "weight" numeric(12,3) NOT NULL,
  "parcelquantity" int4 NOT NULL,
  "trash" bool,
  "cost" numeric(12,2),
  "recipientemail" varchar(100),
  "recipientmobilenumber" varchar(50),
  "options" varchar(500),
  "returnshipmentid" varchar(255),
  "recipientprovince" varchar(100),
  "recipientstreet1" varchar(35),
  "recipientstreet2" varchar(35),
  "recipientstreet3" varchar(35),
  "recipientcompany" varchar(255),
  "sortcode" varchar(255),
  "groupid" varchar(255),
  "createaging" numeric(28,6),
  "parcels" varchar(50000),
  "command" varchar(255),
  "labelkey" varchar(8000),
  "labelurl" varchar(8000),
  "shippingnumber" varchar(36),
  "account" varchar(255),
  "cn23_url" varchar(255),
  "sms_pushed" bool,
  "online_at" timestamp,
  "supplier_weight" numeric(12,3),
  PRIMARY KEY("id", "trackingnumber")
)
    """
    return get_meta(sql, flt_columns_checker)


def get_cms_parcel():
    sql = """
    CREATE TABLE "dk"."cms_parcel" (
  "id" int4 NOT NULL,
  "created_at" timestamp NOT NULL,
  "updated_at" timestamp NOT NULL,
  "tracking_number" varchar(255) NOT NULL,
  "shipping_number" varchar(255),
  "transporter" varchar(255) NOT NULL,
  "application" varchar(255) NOT NULL,
  "transporter_account_id" varchar(255),
  "client_id" varchar(255),
  "channel" varchar(255),
  "status" varchar(255) NOT NULL,
  "last_timestamps" timestamp,
  "last_event" varchar(255),
  "last_description" text,
  "aging" numeric(28,6),
  "declared_at" timestamp NOT NULL,
  "transferred_at" timestamp,
  "arrived_at" timestamp,
  "returned_at" timestamp,
  "deleted_at" timestamp,
  "is_arrived" bool,
  "is_returned" bool,
  "is_lost" bool,
  "error" varchar(8000),
  "receiver_country_code" varchar(255),
  "receiver_postal_code" varchar(255),
  "receiver_city" varchar(255),
  "insurance_value" numeric(28,6) NOT NULL,
  "sync" bool,
  "api_version" varchar(255) NOT NULL,
  "supplier_weight" int4,
  "hub_code" varchar(255),
  PRIMARY KEY("id", "tracking_number")
)
    """
    return get_meta(sql, flt_columns_checker)


def get_cms_bill_purchase_detail():
    sql = """
    CREATE TABLE "dk"."cms_bill_purchase_detail" (
  "id" int4 NOT NULL,
  "invoice_number" varchar(255) NOT NULL,
  "tracking_number" varchar(255) NOT NULL,
  "shipping_number" varchar(255) NOT NULL,
  "weight" numeric(10,3) NOT NULL,
  "postal_code" varchar(255) ,
  "shipping_fee" numeric(10,2) NOT NULL,
  "fuel_fee" numeric(12,6) NOT NULL,
  "extra_fee" numeric(10,2) NOT NULL,
  "vat" numeric(10,2) NOT NULL,
  "invoiced_at" timestamp,
  "created_at" timestamp,
  "updated_at" timestamp,
  "extra_fee_detail" json,
  "region_range" varchar(255) ,
  "weight_range" numeric(10,3),
  "transporter_account_id" varchar(255) NOT NULL,
  "bill_id" int4 NOT NULL,
  "route" varchar(255) ,
  "country_code" varchar(255) ,
  "shipping_fee_after_remise" numeric(10,3),
  "rough_weight" numeric(10,3),
  "weight_type" varchar ,
  "product" varchar(20) ,
  "receive_country_code" char(2),
  primary key ("id", "tracking_number")
)
    """
    return get_meta(sql, flt_columns_checker)


def get_tms_parcel():
    sql = """"
    CREATE TABLE "dk"."tms_parcel" (
  "created_at" timestamp,
  "updated_at" timestamp,
  "updated_by" varchar(255),
  "created_by" varchar(255),
  "id" varchar(255) NOT NULL,
  "tracking_number" varchar(255) NOT NULL,
  "twb" varchar(255) NOT NULL,
  "uld_id" varchar(255),
  "box_id" varchar(255),
  "destination_code" varchar(50),
  "sorting_start_at" timestamp,
  "sorting_end_at" timestamp,
  "process_at" timestamp,
  "pre_stock_out_at" timestamp,
  "stock_out_at" timestamp,
  "sorting_box_id" varchar(255),
  "transferred_at" timestamp,
  "weight" numeric(19,2),
  PRIMARY KEY("id", "tracking_number")
)
    """
    return get_meta(sql, flt_columns_checker)


tasks = dict()
tasks['oms_parcels'] = get_oms_parcels
tasks['oms_shipments'] = get_oms_shipments
tasks['cms_parcel'] = get_cms_parcel
tasks['cms_bill_purchase_detail'] = get_cms_bill_purchase_detail
tasks['tms_parcel'] = get_tms_parcel


def run(task_key, output_path, partition_key):
    spark = SparkSession.builder.getOrCreate()

    task = tasks[task_key]
    columns, value_funcs = task()

    vals = list()
    batch_count = 0
    for i in range(1, total_rows):
        vv = [func() for func in value_funcs]
        vals.append(tuple(vv))
        if i % unit_row == 0 or i == total_rows-1:
            df = spark.createDataFrame(vals, columns)
            new_partition_key = f"p{partition_key}"
            df = df.withColumn(new_partition_key, df[partition_key])
            output = f"{output_path}/{task_key}/{batch_count}/{task_key}.parquet"
            # df.write.mode(saveMode="append").partitionBy(
            #     new_partition_key).parquet(output)

            dyn_df = DynamicFrame.fromDF(df, glueContext, "nested")
            dyn_df = dyn_df.coalesce(1)
            # Script generated for node S3 bucket
            S3bucket_node = glueContext.write_dynamic_frame.from_options(
                frame=dyn_df,
                connection_type="s3",
                format="parquet",
                connection_options={"path": output,
                                    "partitionKeys": [new_partition_key]},
                transformation_ctx="S3bucket_node",
            )

            batch_count += 1
            print(f"=========> {batch_count}")
            vals = list()


run(args['tablename'], args['s3outpath'], args['partition'])
