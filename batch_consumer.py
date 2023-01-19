import boto3
import json
import os
import sys
import uuid
from json import loads
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *


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


    def clean_data(self):
        '''
        Cleans data from the S3 bucket and displayed in a dataframe.
        '''
        os.environ["PYSPARK_PYTHON"]=sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"]=sys.executable
        os.environ["PYSPARK_SUBMIT_ARGS"]="--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"

        #start the spark session
        config = SparkConf() \
        .setAppName("S3toSpark") \

        sc = SparkContext(conf=config)
        spark = SparkSession(sc).builder.appName("S3App").getOrCreate()
        
        #credentials provided for S3 bucket
        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set("fs.s3a.access.key", "A*******************")
        hadoopConf.set("fs.s3a.secret.key", "/**************************")
        hadoopConf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") 
        
        #reads data from s3 bucket
        df = spark.read.json(f"s3a://{self.s3_bucket_name}/*.json")
        
        #cleans data, removes null values
        df = df.withColumn( "follower_count", regexp_replace(df.follower_count, "User Info Error", "N/A"))
        df = df.withColumn("tag_list", regexp_replace(df["tag_list"], "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "N/A"))
        df = df.withColumn("image_src", regexp_replace(df["image_src"], "Image src error.", "N/A"))
        df = df.withColumn("title", regexp_replace(df["title"], "No Title Data Available", "N/A"))
        df = df.withColumn("description", regexp_replace(df["description"], "No description available Story format", "N/A"))
        #cleans follower count and turns it into an integer
        df = df.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
        df = df.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))
        df = df.withColumn("follower_count", col("follower_count").cast("integer"))

        df.sort("follower_count").show()


if __name__ == '__main__':
    test = Consumer()
    test.dump_data()
    test.clean_data()
