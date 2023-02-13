import warnings
warnings.filterwarnings("ignore") 
from message_struc_pb2 import AverageRating, AverageRatings
from helper_file import Consumer, DataSink_2


def run(framework="rabbitmq", n=1):
    topic = "top_ratings"
    
    datasink_2 = DataSink_2(path="./src/datasink/")
    
    if framework == 'kafka':
        consumer_2 = Consumer(datasink_2, framework='kafka', host_name="broker1", port=9093)
    
    if framework == 'rabbitmq':
        consumer_2 = Consumer(datasink_2, framework='rabbitmq', host_name="rabbitmq1", port=5672)

    consumer_2.consume(topic, AverageRatings, n=n)