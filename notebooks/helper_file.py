import pika
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
import datetime
import json
import uuid
import time
import os



class Producer_1():
    '''
    Producer class that connects to a specified framework and produces messages on the network
    Methods:
        setup_connections: Sets up the connection to the framework. Currently implemented are kafka and rabbitmq.
        produces: Sends recieved messages to the initialized framework
    '''
    
    def __init__(self, framework='kafka', host_name="broker1", port=9093):
        self.framework = framework
        self.host_name = host_name
        self.port = port
        self.servers = host_name + ":" + str(port)
        self.setup_connection()
        
        
    def setup_connection(self):
        '''
        Sets up the connecton to the specified framework.
        '''
        producer = None
        try:
            if self.framework == "kafka":
                print("Setup kafka connection")
                self.producer = KafkaProducer(
                    bootstrap_servers=self.servers,
                    api_version=(0, 10)
                 )
                
            elif self.framework == "rabbitmq":
                print("Setup rabbitmq connection")
                connection_parameters = pika.ConnectionParameters(self.host_name, self.port)
                self.connection = pika.BlockingConnection(connection_parameters)
                self.channel = self.connection.channel()
            
            else:
                print(f"Framework {self.framework} not implemented!")

        except Exception as ex:
            print(f"Exception while connecting {self.framework}: {ex}")
        

    def produce(self, topic_name, message):
        '''
        Sends recieved messages to the initialized framework
        Args: 
            topic_name (str): specifies the topic / exchange where to send the data within the framweork
            message (dict): message as json-like dictionary
        '''
        try:
            if self.framework == "kafka":
                key = str(uuid.uuid4())
                value = json.dumps(message)
                key_bytes = bytes(key, encoding='utf-8')
                value_bytes = bytes(value, encoding='utf-8')
                # value_bytes = message.SerializeToString()
                self.producer.send(topic_name, key=key_bytes, value=value_bytes)
                self.producer.flush()
                
            elif self.framework == "rabbitmq":
                message = json.dumps(message)
                # self.channel.exchange_declare(exchange=topic_name, exchange_type='direct')
                self.channel.queue_declare(queue=topic_name)
                self.channel.basic_publish(exchange="", routing_key=topic_name, body=message)
                # self.channel.basic_publish(exchange="", routing_key=topic_name, body=message.SerializeToString())
            
        except Exception as ex:
            print(f'Exception while producing message: {ex}')
            
            
            
# class Producer_2(Producer_1):
    
#     def __init__(self, framework='kafka', servers='broker1:9093'):
#         super().__init__(framework, servers)
        
#     def produce(self, topic_name, message):
#         try:
#             if self.framework == "kafka":
#                 pass
#                 pd.read_csv()
#                 key = str(uuid.uuid4())
#                 value = json.dumps(message)
#                 key_bytes = bytes(key, encoding='utf-8')
#                 value_bytes = bytes(value, encoding='utf-8')
#                 # value_bytes = message.SerializeToString()
#                 self.producer.send(topic_name, key=key_bytes, value=value_bytes)
#                 self.producer.flush()
                
#             elif self.framework == "rabbitmq":
#                 pass
#                 message = json.dumps(message)
#                 self.channel.queue_declare(queue=topic_name)
#                 self.channel.basic_publish(exchange="", routing_key=topic_name, body=message)
#                 # self.channel.basic_publish(exchange="", routing_key=topic_name, body=message.SerializeToString())
            
#         except Exception as ex:
#             print(f'Exception while producing message: {ex}')
            
        
            
            
class DataSink():
    '''
    DataSink class that calculates the most rated producs and the average rating per product and save the results as csv
    '''
    
    def __init__(self, path="./src/datasink/"):
        self.sink_path = path
        self.__ckeck_path()
        self.last_sink = time.time()
        self.history = pd.DataFrame()
        
    
    def __ckeck_path(self):
        '''
        Checks if path to save csv's exists and creates path if not
        '''
        if not os.path.exists(self.sink_path):
            os.makedirs(self.sink_path)
        
        
    def check_period(self, s=60):
        '''
        Checks if a specified time intervall has passed and returns a boolean value
        Args: 
            s (int): nr of seconds per time intervall
        '''
        return  self.last_sink + 60 <= time.time() 
    
    
    def add_entry(self, message):
        '''
        Adds a new message to the history df
        Args: 
            message (dict) message as json like dict
        '''
        self.history = self.history.append(message, ignore_index=True)
            
            
    def __calculate_recent_popular(self):
        '''
        Calculates the nr of ratings and the average rating score per product and returns it as pandas.DataFrame
        '''
        if len(self.history) > 0:
            df = self.history
            df = df[["asin", "overall"]].groupby("asin").agg(mean_overall=("overall","mean"), count=("overall","count"))
            df = df.sort_values(["count", "mean_overall"], ascending=False)
            df = df.reset_index()
            return df
    
    
    def __delete_history(self):
        '''
        Resets the self.history atribute to an empty dataframe
        '''
        self.history = pd.DataFrame()
        
        
    def sink_data(self):
        df = self.__calculate_recent_popular()
        df.to_csv(self.sink_path + time.strftime('%Y_%m_%d_%H_%M_%S', time.localtime()) + ".csv", index=False)
        self.last_sink = time.time()
        self.__delete_history()
        print("Data sink was successful!")
        
        
    
class Consumer_1():
    
    def __init__(self, framework='kafka', host_name="broker1", port=9093):
        self.framework = framework
        self.host_name = host_name
        self.port = port
        self.servers = host_name + ":" + str(port)
        self.setup()
        self.data_sink = DataSink()
        
        
    def setup(self):
        '''
        Sets up the connecton to the specified framework.
        '''
        try: 
            if self.framework == "kafka":
                self.consumer = KafkaConsumer(
                    auto_offset_reset='earliest',
                    bootstrap_servers=self.servers, 
                    api_version=(0, 10), 
                    # value_deserializer = json.loads, # change with protobuf
                    consumer_timeout_ms=float("inf")
                )
                
            elif self.framework == "rabbitmq":
                connection_parameters = pika.ConnectionParameters(self.host_name, self.port)
                self.connection = pika.BlockingConnection(connection_parameters)
                self.channel = self.connection.channel()

        except Exception as ex:
            print(f"Exception while connecting {self.framework}: {ex}")
            
            
    def consume(self, topic_name):
        '''
        Gets recieved messages to the initialized framework
        '''
        try:
            if self.framework == "kafka":
                self.consumer.subscribe(topic_name)
                
                for i, msg in enumerate(self.consumer):
                    message = json.loads(msg.value)
                    
                    self.data_sink.add_entry(message)
                    if self.data_sink.check_period(60):
                        self.data_sink.sink_data()
                        
            elif self.framework == "rabbitmq":
                print("rabbitmq consume not implemented")
                
                def callback(ch, methods, properties, body):
                    message = json.loads(body)
                    
                    # call datasink
                    self.data_sink.add_entry(message)
                    if self.data_sink.check_period(60):
                        self.data_sink.sink_data()
                    
                    ch.basic_ack(delivery_tag=methods.delivery_tag)
                
                queue = self.channel.queue_declare(queue=topic_name)
#                 queue_name = queue.method.queue
#                 print(queue_name)
                
                # self.channel.queue_bind(exchange="", queue=topic_name, routing_key=topic_name)
                self.channel.basic_consume(queue=topic_name, on_message_callback=callback)
                
                self.channel.start_consuming()
            
        except Exception as ex:
            print(f'Exception while consuming message: {ex}')
            
            

