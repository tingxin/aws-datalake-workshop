docker run -it --rm zendesk/maxwell bin/maxwell --user=admin \
    --password=Demo1234 --host=demo.c6lwjjfhbm6a.rds.cn-northwest-1.amazonaws.com.cn --producer=kafka \
    --kafka.bootstrap.servers=b-3.demo-cluster-1.9z77lu.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092 --kafka_topic=maxwell