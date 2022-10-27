import warnings
warnings.filterwarnings("ignore") 
from helper_file import Consumer, DataSink_1
from message_struc_pb2 import Rating


def run(framework='rabbitmq', n=1):
    topic = "live_ratings"
    
    datasink_1 = DataSink_1(path="./src/datasink/")
    
    if framework == 'kafka':
        consumer_1 = Consumer(datasink_1, framework='kafka', host_name="broker1", port=9093)
    
    if framework == 'rabbitmq':
        consumer_1 = Consumer(datasink_1, framework='rabbitmq', host_name="rabbitmq1", port=5672)

    consumer_1.consume(topic, Rating, n=n)
    