from multiprocessing import Queue, Process
from threading import get_ident
import time
import json
import time
from helper_file import Producer
from message_struc_pb2 import Rating

class MultiprocessProducer:
    
    def __init__(self, producer, file, hz, send_n_ratings, bn_s=1):
        self.producer = producer
        self.file = file
        self.hz = hz
        self.send_n_ratings = send_n_ratings
        self.bottleneck_seconds = bn_s
        self.queue1 = Queue()
        self.queue2 = Queue()
        
        
    def check_data(self):
        '''
        Checking data needs some time :)
        This is a placeholder function that creates a bottleneck
        '''
        sum([i for i in range(10**7)])
 

    def get_ratings(self):
        '''
        Loads data from the source into the into the first queue
        '''
        print(f'get_ratings: Running on thread {get_ident()}')
        # generate work
        with open(self.file) as f:
            for i, line in enumerate(f):
                message = json.loads(line)
                self.queue1.put(message)
                time.sleep(1/self.hz)
                
                if i+1 == self.send_n_ratings:
                    break    
        # all done
        self.queue1.put(None)
        return
    

    def check_ratings(self):
        
        print(f'check_ratings: Running on thread {get_ident()}')
        # consume work
        while True:
            # get a unit of work
            try:
                message = self.queue1.get()
            except self.queue1.Empty:
                time.sleep(0.5)
                continue
            # check for stop
            if message is None:
                self.queue1.put(None)
                self.queue2.put(None)
                return

            # bottleneck 
            self.check_data()
            self.queue2.put(message)


    def publish_ratings(self, topic):
        print(f'publish_ratings: Running on thread {get_ident()}')
        # consume work
        i = 0
        while True:
            try:
                message = self.queue2.get()
            except self.queue2.Empty:
                time.sleep(0.5)
                continue
            if message is None:
                return
                
            rating = Rating()
            rating.reviewerID = str(message["reviewerID"])
            rating.asin = str(message["asin"])
            rating.overall = int(message["overall"])
            rating.reviewText = str(message["reviewText"])
 
            self.producer.produce(topic, message=rating)
            if i % (self.hz*1) == 0:
                print(f"{i} ratings sent.")
            i += 1
            time.sleep(1/self.hz)