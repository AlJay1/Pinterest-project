import json
from json import loads
from kafka import KafkaConsumer
import tempfile
import boto3
import uuid


class Consumer():


    def __init__(self):
        self.s3_resource = boto3.resource('s3')
        self.s3_bucket_name = self.name_bucket()

    
        
    
    def name_bucket(self):
        uidd4 = str(uuid.uuid4())
        return f"pinterest-data-{uidd4}"

    def create_bucket(self):
       

        self.s3_resource.create_bucket(Bucket=self.s3_bucket_name,
                                CreateBucketConfiguration={
                                    'LocationConstraint':'us-east-2'})

    def dump_data(self):

        batch_consumer = KafkaConsumer(
            'PinterestPipeline',
            bootstrap_servers=['172.29.128.206:9092'],
            auto_offset_reset = 'latest',
            value_deserializer = lambda x:json.loads(x),
            api_version=(0, 10, 1)
            )

        for number,batch_message in enumerate(batch_consumer):


            data_object = self.s3_resource.Object(self.s3_bucket_name, f"message_{number}.json")
            data_object.put(Body=(bytes(json.dumps(batch_message.value).encode("utf-8"))))          
            



if __name__ == '__main__':
    test = Consumer()
    test.create_bucket()
    test.dump_data()

    

#172.21.225.129