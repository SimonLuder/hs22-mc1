import json
import time
from helper_file import Producer
from message_struc_pb2 import Rating


def run(framework='rabbitmq', n=None, hz=5):
    
    topic = "live_ratings"
    file = "src/json/Toys_and_Games_5-short.json"
    
    if framework == 'kafka':
        producer_1 = Producer(framework='kafka', host_name="broker1", port=9093)
    
    if framework == 'rabbitmq':
        producer_1 = Producer(framework='rabbitmq', host_name="rabbitmq1", port=5672)
    
    with open(file) as f:
        for i, line in enumerate(f):
            message = json.loads(line)

            rating = Rating()
            rating.reviewerID = str(message["reviewerID"])
            rating.asin = str(message["asin"])
            rating.overall = int(message["overall"])
            rating.reviewText = str(message["reviewText"])

            producer_1.produce(topic, message=rating)
            if i % (hz*10) == 0:
                print(f"{i} ratings sent.")
            time.sleep(1/hz)
            if n:
                if i+1 >= n:
                    return
            
            