import sys
from faker import Faker
from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext
from random import random
# total_rows = 50000000
# unit_row = 10000

total_rows = 50
unit_row = 10

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
        elif v.startswith("numeric") or v.startswith("int"):
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


tasks = dict()
tasks['oms_parcels'] = get_oms_parcels
tasks['oms_shipments'] = get_oms_shipments


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
            df.write.mode(saveMode="append").partitionBy(
                new_partition_key).parquet(output)

            batch_count += 1
            print(f"=========> {batch_count}")
            vals = list()


# run(args['tablename'], args['s3outpath'], args['partition'])

# c, d = get_oms_shipments()
# print(c)


run("oms_parcels", "/home/ec2-user/workshop/aws-datalake-workshop/temp",
    "createdat")
