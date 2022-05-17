from confluent_kafka import Consumer
from setting import BootStrap_Servers
import sys

SASL_USERNAME = "admin"
SASL_PASSWORD = "admin-Demo1234"

conf = {
    'bootstrap.servers': BootStrap_Servers,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': SASL_USERNAME,
    'sasl.password': SASL_PASSWORD,
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'

    # any other config you like ..
}

if __name__ == '__main__':
    if len(sys.argv) > 1:
        topic_name = sys.argv[1]
    else:
        raise ValueError("need param for topic name")

    c = Consumer(**conf)

    c.subscribe([topic_name])

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))
