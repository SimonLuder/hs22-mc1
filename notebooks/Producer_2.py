import json
import time
from helper_file import Producer
import pandas as pd
import os
from message_struc_pb2 import AverageRating, AverageRatings


def get_oldest_file(path):
    list_of_files = os.listdir(path)
    full_path = [f"{path}/{file}" for file in list_of_files if str(file).endswith(".csv")]
    if len(full_path) == 0:
        return None
    oldest_file = min(full_path, key=os.path.getctime)
    return oldest_file

def get_archive_string(s):
    s = s.split("/")
    s.insert(-1, 'archive')
    return "".join([(f"/{i}") for i in s])[1:]


def run(framework='rabbitmq', n=None, hz=1/10):
    
    topic = "top_ratings"
    path = "src/datasink/"
    
    if not os.path.exists(path+"archive/"):
        os.makedirs(path+"archive/")
    
    if framework == 'kafka':
        producer_2 = Producer(framework='kafka', host_name="broker1", port=9093)
    
    if framework == 'rabbitmq':
        producer_2 = Producer(framework='rabbitmq', host_name="rabbitmq1", port=5672)

    j = 0
    if n is None:
        n = -1
    
    while j != n:
        j += 1
        file = get_oldest_file(path)
        if file is not None:
            df = pd.read_csv(file)
            df = df.head(5)
            avg_ratings = AverageRatings()
            for i in range(len(df)):
                single_avg = avg_ratings.average_rating.add()
                single_avg.asin = str(df["asin"][i])
                single_avg.mean_overall = float(df["mean_overall"][i])
                single_avg.count = int(df["count"][i])

            producer_2.produce(topic, message=avg_ratings)
            print(f"File: {file} published")
            os.replace(file, get_archive_string(file))
        time.sleep(1/hz)