# AiCore Pinterest Data Engineering Project

The Pinterest Data Processing Pipeline is a system that ingests, processes, and stores data from the Pinterest API in real-time and batch mode. 
The pipeline has two main components: the batch processing unit and the real-time processing unit.


## Milestone 1 - Briefing

Pinterest struggled to meet their daily deadlines and so needed to implement a new pipeline with the following requirements:
* Flexibilty to add and apply new metrics.
* The ability to create dahboards with both recent and historical data.
* The capability to handle a large volume of data from a rapidly expanding user base.

## Milestone 2 - Configuring the API and Consuming in Kafka

I downloaded the pinterest infrastructure. The ```project_pin_API.py``` file and the ```user_posting_emulation.py``` were run at the same time to begin the simulation

A kafka topic was created with the name ```PinterestPipeline``` and then initialised so it was ready to receive data.

In the producer, the data was converted into bites and then sent to the topic. 

After that, both the ```batch_consumer.py``` and the ```stream_consumer``` was created. At this point they were both identical as shown below and the appropriate changes would be made to each consumer later in the project.


```
stream_consumer = KafkaConsumer(
      'PinterestPipeline',
     bootstrap_servers=['172.21.225.129:9092'],
     auto_offset_reset = 'latest',
     value_deserializer = lambda x:json.loads(x)
 )

stream_consumer.subscribe(topics="PinterestPipeline")

for stream_message in stream_consumer:
    print(stream_message.value)
    print(stream_message.timestamp)

```

## Milestone 3 - Batch processing:Ingest data into the data lake

In the ```batch_consumer.py``` file, a S3 bucket was created with ```name_bucket``` and the ```create_bucket``` functions.
The uuid library was used to create a unique id each time a bucket is created. 

## Milestone 4 - Batch Processiong:Process the data using Spark

In the ```clean_data``` function, an os environemtnt was created for Pyspark_Python, Pyspark_Driver_Python and Pyspark_Submit_Args.

This allowed Apache Spark to interact with the AWS bucket.
It was then configured with the access keys.

Then the data cleaning was performed.
The JSON data was embeddded in a dataframe and the null values were replaced for several columns.
Also the follower_count column was turned into an integer.

## Milestone 5 - Apache Airflow
With Apache Airflow, the data upload onto AWS and the cleaning operations were set to happen once a day.
This is done by running the ```airflow.py``` file. 

The permissions of the bucket had to be updated so files were deleted after 24 hours to prevent the same data being reloaded onto the dataframe.

The DAG is shown below.

```
#creates DAG, scheduled to run once a day at midnight
with DAG(dag_id='batch_consumer_dag',
         default_args=default_args,
         schedule_interval='0 0  * * *',
         catchup=False,
         tags=['batch_process']
         ) as dag:
    #runs the batch_consumer file. In if name == main block only has the functions dumping and cleaning the data

    run_producer_script = BashOperator(
        task_id='run_producer_file',
        bash_command='cd ~/pinterest_project && python3 batch_consumer.py',
        dag=dag)

```

## Milestone 6 - Streaming: Kafka-Spark Integration


## Milestone 7 - Spark Streaming


## Milestone 8 - Streaming: Storage