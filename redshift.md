```
CREATE EXTERNAL SCHEMA MySchema
FROM MSK
IAM_ROLE { default | 'iam-role-arn' }
AUTHENTICATION { none | iam }
CLUSTER_ARN 'msk-cluster-arn';
```
例如
```
CREATE EXTERNAL SCHEMA KafkaSchema
FROM MSK
IAM_ROLE default
AUTHENTICATION none
CLUSTER_ARN 'arn:aws:kafka:ap-northeast-1:515491257789:cluster/demo3/daa40c3d-c647-4837-876f-1d95ad8791dd-3';
```


CREATE MATERIALIZED VIEW MyView AUTO REFRESH YES AS
SELECT "kafka_partition", 
 "kafka_offset", 
 "kafka_timestamp_type", 
 "kafka_timestamp", 
 "kafka_key", 
 JSON_PARSE("kafka_value") as Data, 
 "kafka_headers"
FROM KafkaSchema.redshift;