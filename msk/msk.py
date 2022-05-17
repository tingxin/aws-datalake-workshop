import os
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP_SERVERS=os.gentenv("KAFKA_BOOTSTRAP_SERVERS").split(",")
TOPIC_NAME="the-topic"
SASL_USERNAME=os.gentenv("KAFKA_SASL_USERNAME")
SASL_PASSWORD=os.gentenv("KAFKA_SASL_PASSWORD")

def consume():
  consumer = KafkaConsumer(TOPIC_NAME, security_protocol="SASL_SSL",  sasl_mechanism="SCRAM-SHA-512", sasl_plain_username=SASL_USERNAME, sasl_plain_password=SASL_PASSWORD, bootstrap_servers=BOOTSTRAP_SERVERS)
  for msg in consumer:
    print (msg)

    
def produce():
  producer = KafkaProducer(security_protocol="SASL_SSL",  sasl_mechanism="SCRAM-SHA-512", sasl_plain_username=SASL_USERNAME, sasl_plain_password=SASL_PASSWORD, bootstrap_servers=BOOTSTRAP_SERVERS)
  producer.send(TOPIC_NAME, b'some_message_bytes')
