target=/usr/lib/flink/lib/
# 下载hudi flink bundle jar, 这个是我针对EMR6.4.0编译好的，用这个就可以
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/hudi-flink-bundle_2.12-0.10.1.jar
mv hudi-flink-bundle_2.12-0.10.1.jar /usr/lib/flink/lib/
# kafka connector
wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka_2.12/1.13.1/flink-connector-kafka_2.12-1.13.1.jar
mv flink-connector-kafka_2.12-1.13.1.jar  ${target}
# hudi新版本已经把hive-exec从bundle包抽出来了，这样做很好，对Metastore兼容性会更强，比如Glue，下面三个copy 当我们需要将hudi的表自动同步到Hive Metastore或者Glue Metastore是需要。 如果不需要实时自动同步，不用下面三个 包。
cp /usr/lib/hive/lib/libthrift-0.9.3.jar ${target}
cp /usr/lib/hive/lib/hive-exec.jar ${target}
cp /usr/lib/hive/lib/commons-lang-2.6.jar ${target}
# glue catalog jar，使用这个jar实现的AWSGlueDataCatalogHiveClientFactory, 如果需要将数据同步到Glue Catalog需要这个Jar
cp /usr/lib/hive/auxlib/aws-glue-datacatalog-hive3-client.jar ${target}
