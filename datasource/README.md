## 创建数据
4. 创建数据表
```
mysql -h demo.c6lwjjfhbm6a.rds.cn-northwest-1.amazonaws.com.cn -P 3306 -u admin -p

create database demo;

use demo;

CREATE TABLE IF NOT EXISTS `order` (
    order_id INT AUTO_INCREMENT NOT NULL,
    user_mail varchar(20) NOT NULL,
    status char(10) NOT NULL, 
    good_count INT NOT NULL,
    city varchar(20) NOT NULL,
    amount FLOAT NOT NULL,
    create_time datetime NOT NULL,
    update_time datetime NOT NULL,
    PRIMARY KEY (`order_id`)
);
```
5. 插入假数据
```
python3 mock.py
```


msk
```
export KAFKA_OPTS=-Djava.security.auth.login.config=/home/ec2-user/workshop/aws-datalake-workshop/datasource/temp/users_jaas.conf


aws kafka get-bootstrap-brokers --cluster-arn 


bin/kafka-acls.sh --authorizer-properties zookeeper.connect=z-3.demo-kafka.f9vui7.c4.kafka.cn-northwest-1.amazonaws.com.cn:2181,z-1.demo-kafka.f9vui7.c4.kafka.cn-northwest-1.amazonaws.com.cn:2181,z-2.demo-kafka.f9vui7.c4.kafka.cn-northwest-1.amazonaws.com.cn:2181 --add --allow-principal "User:CN=admin" --operation Read --group=* --topic sasl





bin/kafka-acls.sh --authorizer-properties zookeeper.connect=z-3.demo-kafka.f9vui7.c4.kafka.cn-northwest-1.amazonaws.com.cn:2181,z-1.demo-kafka.f9vui7.c4.kafka.cn-northwest-1.amazonaws.com.cn:2181,z-2.demo-kafka.f9vui7.c4.kafka.cn-northwest-1.amazonaws.com.cn:2181 --add --allow-principal "User:CN=admin" --operation Read --group=* --topic sasl



bin/kafka-acls.sh --authorizer-properties zookeeper.connect=z-3.demo-kafka.f9vui7.c4.kafka.cn-northwest-1.amazonaws.com.cn:2181,z-1.demo-kafka.f9vui7.c4.kafka.cn-northwest-1.amazonaws.com.cn:2181,z-2.demo-kafka.f9vui7.c4.kafka.cn-northwest-1.amazonaws.com.cn:2181 --add --allow-principal "User:admin" --operation all --topic sasl
```