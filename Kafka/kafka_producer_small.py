import csv
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='kafka:9092',
                         value_serializer=lambda x: x.encode('utf-8'))

with open('brightkite_small.csv') as file:
    reader = csv.reader(file)
    for row in reader:
        message = ','.join(row)
        timestamp = str(datetime.now())
        message_with_timestamp = f"{message},{timestamp}"
        producer.send('topic3', value=message_with_timestamp)

producer.flush()
