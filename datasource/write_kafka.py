from itsdangerous import json
from kafka import KafkaProducer
from time import sleep
from mock import gen, DataType
import json
import sys
from setting import BootStrap_Servers

behaviour_schema = {
    "user_mail": (DataType.Enum, ('barry.xu@163.com', 'dandan@qq.com', 'pony@qq.com', 'focus@qq.com', 'guest', 'guest', 'guest', 'guest', 'guest', 'guest', 'guest')),
    "event_type": (DataType.Enum, ('click', 'page_enter', 'page_leave')),
    "resource_type": (DataType.Enum, ('news', 'product', 'vedio')),
    "resource_content": (DataType.STR,),
    "event_time":  (DataType.DATETIME,)
}

creator = gen(columns=behaviour_schema, increment_id="resource_id",
              interval_min=100, interval_max=1000)


def send_success(self, *args, **kwargs):
    """异步发送成功回调函数"""
    print('save success')
    return


def send_error(self, *args, **kwargs):
    """异步发送错误回调函数"""
    print('error => {0}'.format(*args))
    return


def start_producer(topic):
    producer = KafkaProducer(bootstrap_servers=BootStrap_Servers)
    for item in creator:
        doc = json.dumps(item).encode('utf-8')
        producer.send(topic, doc).add_callback(
            send_success).add_errback(send_error)
        producer.flush()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        topic_name = sys.argv[1]
    else:
        raise ValueError("need param for topic name")
    # read your csv
    start_producer(topic_name)
