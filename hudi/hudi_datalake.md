ip
```
172.31.43.238
```
1. 启动mysql 以及 maxwell [在另外一个项目]
```
mysql -h database-2.cluster-cqshokmqgqfv.ap-northeast-1.rds.amazonaws.com -P 3306 -u demo -p

docker run -it --rm zendesk/maxwell bin/maxwell --user=admin \
    --password=Demo1234 --host=demo.c6lwjjfhbm6a.rds.cn-northwest-1.amazonaws.com.cn --producer=kafka \
    --kafka.bootstrap.servers=b-1.demo-cluster-1.9z77lu.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092 --kafka_topic=maxwell
```

2. run flink session
```
bash run_flink_session.sh
```

3. 启动flink sql
```
/usr/lib/flink/bin/sql-client.sh -s application_1648290404228_0006

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
7. 验证测试. 
```
使用flink sql 测试,使用hive
select sum(good_count) from flink_hudi_order_ods;

```
8. 使用spark sql 查询
```
sudo wget http://maven.aliyun.com/nexus/content/groups/public/org/apache/hudi/hudi-spark3.1.2-bundle_2.12/0.10.1/hudi-spark3.1.2-bundle_2.12-0.10.1.jar .
sudo wget http://maven.aliyun.com/nexus/content/groups/public/org/apache/spark/spark-avro_2.12/3.1.2/spark-avro_2.12-3.1.2.jar .

spark-shell \
--jars ./hudi-spark3.1.2-bundle_2.12-0.10.1.jar,spark-avro_2.12-3.1.2.jar \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
--conf 'spark.dynamicAllocation.enabled=false'

spark.sql("select sum(good_count) from flink_hudi_order_ods").show()
```

8. 手动提交复杂spark jar任务
```
wget 172.31.43.238:5016/spark-scala-examples-1.0-SNAPSHOT.jar
spark-submit \
    --deploy-mode cluster \
    --master yarn \
    --class com.tingxin.app.CheckDws \
    --jars ./hudi-spark3.1.2-bundle_2.12-0.10.1.jar,spark-avro_2.12-3.1.2.jar \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.dynamicAllocation.enabled=false' \
    ./spark-scala-examples-1.0-SNAPSHOT.jar

```
airflow
```
extra='{"key_file": "/usr/local/airflow/.ssh/id_rsa", "no_host_key_check": true}'
```
