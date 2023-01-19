import boto3
import json
import uuid
from json import loads
from kafka import KafkaConsumer



class Consumer():
    """
    Consumer class that creates an S3 bucket and consumes messages from a Kafka topic.
    """

    def __init__(self):
        """
        Initialize the Consumer class by creating a boto3 S3 resource and setting the S3 bucket name.
        """
        self.s3_resource = boto3.resource('s3')
        self.s3_bucket_name = self.name_bucket()


    def name_bucket(self):
        """
        Generates a unique name for the S3 bucket.
        """
        uidd4 = str(uuid.uuid4())
        return f"pinterest-data-{uidd4}"


    def create_bucket(self):
        """
        Creates an S3 bucket with the specified name and region
        """
        self.s3_resource.create_bucket(Bucket=self.s3_bucket_name,
                                CreateBucketConfiguration={
                                    'LocationConstraint':'us-east-2'})

    def dump_data(self):
        """
        Consumes messages from a Kafka topic, prints the message, and saves the message to an S3 bucket.
        """
        #Kafka consumer configuration
        batch_consumer = KafkaConsumer(
            'PinterestPipeline',
            bootstrap_servers=['172.29.128.206:9092'],
            auto_offset_reset = 'earliest',
            value_deserializer = lambda x:json.loads(x),
            api_version=(0, 10, 1)
            )
        #consuming messages and saving to s3
        for number,batch_message in enumerate(batch_consumer, 1):
            print(batch_message.value)
            #creating a new object in the S3 bucket for each message
            data_object = self.s3_resource.Object(self.s3_bucket_name, f"message_{number}.json")
            #encoding message into bytes
            data_object.put(Body=(bytes(json.dumps(batch_message.value).encode("utf-8"))))          


if __name__ == '__main__':
    test = Consumer()
    test.dump_data()