from confluent_kafka import Producer
from setting import BootStrap_Servers
from mock import gen, DataType
import json
import sys


SASL_USERNAME = "admin"
SASL_PASSWORD = "admin-Demo1234"

conf = {
    'bootstrap.servers': BootStrap_Servers,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': SASL_USERNAME,
    'sasl.password': SASL_PASSWORD,
    # any other config you like ..
}

p = Producer(**conf)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))


behaviour_schema = {
    "user_mail": (DataType.Enum, ('barry.xu@163.com', 'dandan@qq.com', 'pony@qq.com', 'focus@qq.com', 'guest', 'guest', 'guest', 'guest', 'guest', 'guest', 'guest')),
    "event_type": (DataType.Enum, ('click', 'page_enter', 'page_leave')),
    "resource_type": (DataType.Enum, ('news', 'product', 'vedio')),
    "resource_content": (DataType.STR,),
    "event_time":  (DataType.DATETIME,)
}


creator = gen(columns=behaviour_schema, increment_id="resource_id",
              interval_min=500, interval_max=900)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        topic_name = sys.argv[1]
    else:
        raise ValueError("need param for topic name")

    for item in creator:
        doc = json.dumps(item).encode('utf-8')
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        p.produce(topic_name, doc, callback=delivery_report)
