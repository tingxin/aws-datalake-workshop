CREATE TABLE mysql_order1 (
  order_id BIGINT,
  user_mail STRING,
  status STRING, 
  good_count BIGINT,
  city STRING,
  amount DECIMAL(10, 2),
  create_time STRING,
  update_time STRING,
  PRIMARY KEY(order_id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'demo.c6lwjjfhbm6a.rds.cn-northwest-1.amazonaws.com.cn',
  'username' = 'admin',
  'password' = 'Demo1234',
  'port' = '3306',
  'database-name' = 'demo',
  'table-name' = 'order'
);


CREATE TABLE mongo_product5 (
  _id STRING, 
  product STRING,
  desc STRING,
  PRIMARY KEY(_id) NOT ENFORCED
) WITH (
  'connector' = 'mongodb-cdc',
  'hosts' = 'demo3.cluster-c6lwjjfhbm6a.docdb.cn-northwest-1.amazonaws.com.cn:27017',
  'username' = 'root',
  'password' = 'Demo1234',
  'database' = 'default',
  'connection.options'='readPreference=primary',
  'collection' = 'product'
);

CREATE TABLE mongo_product_ex (
  _id STRING, 
  product STRING,
  desc STRING,
  PRIMARY KEY(_id) NOT ENFORCED
) WITH (
  'connector' = 'mongodb-cdc',
  'hosts' = '172.31.43.238:27017',
  'username' = 'admin',
  'password' = '123456',
  'database' = 'default',
  'connection.options'='readPreference=primary',
  'collection' = 'product'
);


CREATE TABLE flink_mongo_order_test4(
_id STRING, 
product STRING,
desc STRING,
ts TIMESTAMP(3),
logday VARCHAR(255),
hh VARCHAR(255)
)PARTITIONED BY (`logday`,`hh`)
WITH (
'connector' = 'hudi',
'path' = 's3://tx-workshop/rongbai/flink/mongo_order_test4/',
'table.type' = 'COPY_ON_WRITE',
'write.precombine.field' = 'ts',
'write.operation' = 'upsert',
'hoodie.datasource.write.recordkey.field' = '_id',
'hive_sync.enable' = 'true',
'hive_sync.table' = 'flink_mongo_order_test4',
'hive_sync.mode' = 'HMS',
'hive_sync.use_jdbc' = 'false',
'hive_sync.username' = 'hadoop',
'hive_sync.partition_fields' = 'logday,hh',
'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
);


insert into flink_mongo_order_test4 select * ,CURRENT_TIMESTAMP as ts,
DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') as logday, DATE_FORMAT(CURRENT_TIMESTAMP, 'hh') as hh from mongo_product5;