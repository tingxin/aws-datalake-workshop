from confluent_kafka import Producer, KafkaError
import os

# AWS MSK broker and topic configuration
msk_broker = "b-2.demo.bkqn0x.c3.kafka.ap-northeast-1.amazonaws.com:9098,b-1.demo.bkqn0x.c3.kafka.ap-northeast-1.amazonaws.com:9098,b-3.demo.bkqn0x.c3.kafka.ap-northeast-1.amazonaws.com:9098"
msk_topic = "dered"

# Configure AWS credentials using environment variables
# os.environ['AWS_ACCESS_KEY_ID'] = "your-access-key-id"
# os.environ['AWS_SECRET_ACCESS_KEY'] = "your-secret-access-key"
# os.environ['AWS_SESSION_TOKEN'] = "your-session-token"

# Kafka producer configuration
conf = {
    'bootstrap.servers': msk_broker,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'AWS_MSK_IAM',
    'sasl.jaas.config': 'software.amazon.msk.auth.iam.IAMLoginModule required;'
                       'endpoint={};'.format(msk_broker),
}

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Create Kafka producer
producer = Producer(conf)

# Produce data to the topic
for i in range(10):
    message = "Message {}".format(i)
    producer.produce(msk_topic, key=None, value=message, callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()
