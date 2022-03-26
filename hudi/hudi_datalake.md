ip
```
172.31.9.218
172.31.13.93
172.31.3.71
```
1. 启动mysql 以及 maxwell [在另外一个项目]

2. run flink session
```
bash run_flink_session.sh
```

3. 启动flink sql
```
/usr/lib/flink/bin/sql-client.sh -s application_1648215713226_0013

# result-mode
set sql-client.execution.result-mode=tableau;
# set default parallesim
set 'parallelism.default' = '1';
```

4. 创建kafka table
```
CREATE TABLE kafka_order (
  order_id BIGINT,
  user_mail STRING,
  status STRING, 
  good_count BIGINT,
  city STRING,
  amount DECIMAL(10, 2),
  create_time STRING,
  update_time STRING
) WITH (
 'connector' = 'kafka',
 'topic' = 'maxwell',
 'properties.bootstrap.servers' = 'b-1.demo-cluster-1.9z77lu.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092',
 'properties.group.id' = 'testGroup1',
 'format' = 'maxwell-json'
);
```
5. 同步到hudi以及hive (glue)
```
CREATE TABLE flink_hudi_order_ods(
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
'path' = 's3://tx-workshop/rongbai/flink/flink_hudi_order/',
'table.type' = 'COPY_ON_WRITE',
'write.precombine.field' = 'ts',
'write.operation' = 'upsert',
'hoodie.datasource.write.recordkey.field' = 'order_id',
'hive_sync.enable' = 'true',
'hive_sync.table' = 'flink_hudi_order_ods',
'hive_sync.mode' = 'HMS',
'hive_sync.use_jdbc' = 'false',
'hive_sync.username' = 'hadoop',
'hive_sync.partition_fields' = 'logday,hh',
'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
);
```
6. kafka表的数据导入到hudi 表中
```
insert into flink_hudi_order_ods select * ,CURRENT_TIMESTAMP as ts,
DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') as logday, DATE_FORMAT(CURRENT_TIMESTAMP, 'hh') as hh from kafka_order;
```
7. 使用spark sql 查询
```
sudo wget http://maven.aliyun.com/nexus/content/groups/public/org/apache/hudi/hudi-spark3.1.2-bundle_2.12/0.10.1/ .
sudo wget http://maven.aliyun.com/nexus/content/groups/public/org/apache/spark/spark-avro_2.12/3.1.2/spark-avro_2.12-3.1.2.jar .

spark-shell \
--jars ./hudi-spark3.1.2-bundle_2.12-0.10.1.jar,spark-avro_2.12-3.1.2.jar \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
--conf 'spark.dynamicAllocation.enabled=false'

spark.sql("select sum(good_count) from flink_hudi_order_ods").show()
```

8. 手动提交复杂spark jar任务
```
spark-submit \
    --deploy-mode cluster \
    --master yarn \
    --class com.tingxin.app.Dwd \
    --jars ./spark-avro_2.12-3.1.2.jar \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.dynamicAllocation.enabled=false' \
    ./spark-scala-examples-1.0-SNAPSHOT.jar

```
