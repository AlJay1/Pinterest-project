import json
import multiprocessing
import os
import pyspark
import pyspark.sql.functions
import sys
from json import loads
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *


class Streaming():
        def __init__(self):
                self.kafka_topic_name = "PinterestPipeline"
                self.kafka_bootstrap_servers = '172.18.240.165:9092'
        

        def KafkaConsumer(self):
                stream_consumer = KafkaConsumer(
                self.kafka_topic_name,
                self.bootstrap_servers,
                auto_offset_reset = 'earliest',
                value_deserializer = lambda x:json.loads(x)
                )
                stream_consumer.subscribe(topics = self.kafka_topic_name)


        def spark_stream(self):
                os.environ["PYSPARK_PYTHON"]=sys.executable
                os.environ["PYSPARK_DRIVER_PYTHON"]=sys.executable
                # Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
                os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.5.1 pyspark-shell'

                spark = SparkSession \
                        .builder \
                        .appName("KafkaStreaming ") \
                        .getOrCreate()
                #Only display Error messages in the console.
                spark.sparkContext.setLogLevel("ERROR")
                
                # Construct a streaming DataFrame that reads from topic
                stream_df = spark \
                        .readStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                        .option("subscribe", self.kafka_topic_name) \
                        .option("startingOffsets", "earliest") \
                        .load()

                # Select the value part of the kafka message and cast it to a string.
                stream_df = stream_df.selectExpr("CAST(value as STRING)")

                schema = StructType([
                        StructField("category", StringType(),True),
                        StructField("index", StringType(), True),
                        StructField("unique_id", StringType(), True),
                        StructField("title", StringType(), True),
                        StructField("description", StringType(), True),
                        StructField("follower_count", StringType(), True),
                        StructField("tag_list", StringType(), True),
                        StructField("is_image_or_video", StringType(), True),
                        StructField("image_src", StringType(), True),
                        StructField("downloaded", StringType(), True),
                        StructField("save_location", StringType(), True),
                ])

                df = stream_df.withColumn("value",from_json(col("value"),schema)).select(col("value.*"))


                df = df.withColumn( "follower_count", regexp_replace(df.follower_count, "User Info Error", "N/A"))
                df = df.withColumn("tag_list", regexp_replace(df["tag_list"], "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "N/A"))
                df = df.withColumn("image_src", regexp_replace(df["image_src"], "Image src error.", "N/A"))
                df = df.withColumn("title", regexp_replace(df["title"], "No Title Data Available", "N/A"))
                df = df.withColumn("description", regexp_replace(df["description"], "No description available Story format", "N/A"))

                df = df.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
                df = df.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))
                df = df.withColumn("follower_count", col("follower_count").cast("integer"))


                def real_time(data_frame, epoch_id):
                        df.select(sum("follower_count")).show()
                        df.groupBy("category").sum("follower_count").show()
                        df.select(count(df.category)).show()
                        df.printSchema()

                        df.write() \
                        .mode("append") \
                        .format("jdbc") \
                        .option("driver,org.postgresql.Driver") \
                        .option("url", "jdbc:postgresql://172.18.240.165:5432/pinterest_streaming") \
                        .option("dbtable", "experimental_data") \
                        .option("user","postgres") \
                        .option("password", "The!3arsenals") \
                        .save()

                pass


                # outputting the messages to the console 
                df.writeStream.foreachBatch(real_time) \
                .format("console") \
                .outputMode("append") \
                .start() \
                .awaitTermination()


if __name__ == "__main__":
        test_stream = Streaming()
        test_stream.spark_stream()