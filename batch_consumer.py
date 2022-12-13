import json
from json import loads
from kafka import KafkaConsumer


batch_consumer = KafkaConsumer(
    'PinterestPipeline',
    bootstrap_servers=['172.21.225.129:9092'],
    auto_offset_reset = 'latest',
    value_deserializer = lambda x:json.loads(x)
    )

for batch_message in batch_consumer:
    print(batch_message.value)
    print(batch_message.timestamp)
    
    