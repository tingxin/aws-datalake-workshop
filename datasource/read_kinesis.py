import boto3
from setting import *
import sys
from threading import Thread

DefaultShardIteratorType = 'LATEST'
region = 'cn-northwest-1'


class PrintKinesisThread (Thread):
    def __init__(self, shardId, kinesis_client, stream_name, record_hanler):
        Thread.__init__(self)
        self.stream_name = stream_name
        self.shardId = shardId
        self.kinesis_client = kinesis_client
        self.record_hanler = record_hanler

    def run(self):
        shard_iterator = self.kinesis_client.get_shard_iterator(StreamName=self.stream_name,
                                                                ShardId=self.shardId,
                                                                ShardIteratorType=DefaultShardIteratorType)

        my_shard_iterator = shard_iterator['ShardIterator']
        record_response = self.kinesis_client.get_records(
            ShardIterator=my_shard_iterator, Limit=100)

        while 'NextShardIterator' in record_response:
            record_response = kinesis_client.get_records(
                ShardIterator=record_response['NextShardIterator'], Limit=100)
            if len(record_response['Records']) > 0:
                for record in record_response['Records']:
                    self.record_hanler(record)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        stream_name = sys.argv[1]
    else:
        raise ValueError("need param for stream name")

    session = boto3.Session()
    kinesis_client = session.client('kinesis', region_name=region)
    response = kinesis_client.describe_stream(StreamName=stream_name)
    for shard_info in response['StreamDescription']['Shards']:
        print(shard_info)
        shard_id = shard_info['ShardId']
        printer = PrintKinesisThread(
            shard_id, kinesis_client, stream_name, lambda x: print(f"Got data form {shard_id}==> {x}"))

        print(f"prepare to read data from {stream_name} at shard {shard_id}")
        printer.start()
