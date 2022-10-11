from kafka import KafkaConsumer, KafkaProducer
from IPython.display import clear_output
import matplotlib.pyplot as plt
import pandas as pd
import pika
import json
import uuid
import time
import os



class Producer():
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
            
            
            
class DataSink_1():
    '''
    DataSink class that calculates the most rated producs and the average rating per product and save the results as csv
    '''
    
    def __init__(self, path):
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
        
        
    def __check_period(self, s=60):
        '''
        Checks if a specified time intervall has passed and returns a boolean value
        Args: 
            s (int): nr of seconds per time intervall
        '''
        return  self.last_sink + s <= time.time() 
    
    
    def update_entry(self, message):
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
    
            
    def check_sink_criterias(self):
        return self.__check_period()
       
        
    def sink_data(self):
        df = self.__calculate_recent_popular()
        df.to_csv(self.sink_path + time.strftime('%Y_%m_%d_%H_%M_%S', time.localtime()) + ".csv", index=False)
        self.last_sink = time.time()
        self.history = pd.DataFrame()
        print("Data sink was successful!")
        
        
        
class DataSink_2():
    '''
    DataSink class that calculates the most rated producs and the average rating per product and save the results as csv
    '''
    
    def __init__(self, path):
        # self.sink_path = path
        # self.__ckeck_path()
        self.history = None
        
        
    def check_message_valid(self):
        return  self.last_sink + s <= time.time() 
    
    
    def update_entry(self, message):
        print(message)
        self.history = pd.read_json(message, orient="columns")
        
        
    def __plot_ranking(self):
        df = self.history
        clear_output(wait=True)
        plt.figure(figsize=(10,5))
        plt.bar(df.index, df["count"])
        for x, y, p in zip(df.index, df["count"], df["asin"]):
            plt.text(x, y+1, p, ha="center")
        for x, y, p in zip(df.index, df["count"], df["mean_overall"]):
            plt.text(x, y-3, round(p,1), ha="center")
        plt.title("Most ranked products")
        plt.xlabel("place")
        plt.ylabel("nr of rankings")
        plt.show()
           
            
    def check_sink_criterias(self):
        return True

    
    def sink_data(self):
        self.__plot_ranking()
        self.history = None

        
        
class Consumer():
    
    def __init__(self, data_sink, framework='kafka', host_name="broker1", port=9093 ):
        self.framework = framework
        self.host_name = host_name
        self.port = port
        self.servers = host_name + ":" + str(port)
        self.data_sink = data_sink
        self.setup()
        
        
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
                    print(message)
                    self.data_sink.update_entry(message)
                    if self.data_sink.check_sink_criterias():
                        print("success")
                        self.data_sink.sink_data()
                        
            elif self.framework == "rabbitmq":
                
                def callback(ch, methods, properties, body):
                    message = json.loads(body)
                    # call datasink
                    self.data_sink.update_entry(message)
                    if self.data_sink.check_sink_criterias():
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
            
            

