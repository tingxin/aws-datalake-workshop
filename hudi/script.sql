


```
CREATE TABLE customer (
  email STRING,
  sex STRING,
  level INT,
  PRIMARY KEY (email) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://demo.c6lwjjfhbm6a.rds.cn-northwest-1.amazonaws.com.cn:3306/demo',
   'table-name' = 'customer',
   'username' = 'admin',
   'password' = 'Demo1234'
);
```

4. cdc table
```
CREATE TABLE mysql_raw_order (
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
```

mysql stream table map to hudi
```
CREATE TABLE hudi_raw_order_ods(
order_id BIGINT,
user_mail STRING,
status STRING, 
good_count BIGINT,
city STRING,
amount DECIMAL(10, 2),
create_time STRING,
update_time STRING,
ts TIMESTAMP(3),
logday VARCHAR(255),
hh VARCHAR(255)
)PARTITIONED BY (`logday`,`hh`)
WITH (
'connector' = 'hudi',
'path' = 's3://tx-emr/dongpen/flink/hudi_raw_order_ods/',
'table.type' = 'COPY_ON_WRITE',
'write.precombine.field' = 'ts',
'write.operation' = 'upsert',
'hoodie.datasource.write.recordkey.field' = 'order_id',
'hive_sync.enable' = 'true',
'hive_sync.table' = 'hudi_raw_order_ods',
'hive_sync.mode' = 'HMS',
'hive_sync.use_jdbc' = 'false',
'hive_sync.username' = 'hadoop',
'hive_sync.partition_fields' = 'logday,hh',
'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
);
```

insert into hudi_raw_order_ods select * ,CURRENT_TIMESTAMP as ts,
DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') as logday, DATE_FORMAT(CURRENT_TIMESTAMP, 'hh') as hh from mysql_raw_order;


CREATE TABLE hudi_raw_customer(
email STRING,
sex STRING, 
level INT,
ts TIMESTAMP(3),
logday VARCHAR(255),
hh VARCHAR(255)
)PARTITIONED BY (`logday`,`hh`)
WITH (
'connector' = 'hudi',
'path' = 's3://tx-emr/dongpen/flink/hudi_raw_customer/',
'table.type' = 'COPY_ON_WRITE',
'write.precombine.field' = 'ts',
'write.operation' = 'upsert',
'hoodie.datasource.write.recordkey.field' = 'email',
'hive_sync.enable' = 'true',
'hive_sync.table' = 'hudi_raw_customer',
'hive_sync.mode' = 'HMS',
'hive_sync.use_jdbc' = 'false',
'hive_sync.username' = 'hadoop',
'hive_sync.partition_fields' = 'logday,hh',
'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
);

insert into hudi_raw_customer 
select * ,
CURRENT_TIMESTAMP as ts,
DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') as logday, DATE_FORMAT(CURRENT_TIMESTAMP, 'hh') as hh 
from customer;

table with dimension
```
```
CREATE TABLE hudi_order_with_dimension(
order_id BIGINT,
user_mail STRING,
status STRING, 
good_count BIGINT,
city STRING,
amount DECIMAL(10, 2),
create_time STRING,
update_time STRING,
ts TIMESTAMP(3),
logday VARCHAR(255),
hh VARCHAR(255),
sex STRING
)PARTITIONED BY (`logday`,`hh`)
WITH (
'connector' = 'hudi',
'path' = 's3://tx-emr/dongpen/flink/hudi_order_sex_dwd/',
'table.type' = 'COPY_ON_WRITE',
'write.precombine.field' = 'ts',
'write.operation' = 'upsert',
'hoodie.datasource.write.recordkey.field' = 'order_id',
'hive_sync.enable' = 'true',
'hive_sync.table' = 'hudi_order_sex_dwd',
'hive_sync.mode' = 'HMS',
'hive_sync.use_jdbc' = 'false',
'hive_sync.username' = 'hadoop',
'hive_sync.partition_fields' = 'logday,hh',
'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
);
```


insert into hudi_order_sex_dwd 
select 
m.*,
c.sex
from 
hudi_raw_order_ods as m
left join hudi_raw_customer as c
on m.user_mail = c.email