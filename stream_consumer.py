import json
from json import loads
from kafka import KafkaConsumer


stream_consumer = KafkaConsumer(
     'PinterestPipeline',
    bootstrap_servers=['172.21.225.129:9092'],
    auto_offset_reset = 'latest',
    value_deserializer = lambda x:json.loads(x)
)

for stream_message in stream_consumer:
    print(stream_message.value)
    print(stream_message.timestamp)