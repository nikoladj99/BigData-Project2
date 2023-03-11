import csv
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='kafka:9092',
                         value_serializer=lambda x: (','.join(row)).encode('utf-8'))

with open('brightkite.csv') as file:
    reader = csv.reader(file)
    for row in reader:
        message = ','.join(row)
        producer.send('topic2', value=message.encode('utf-8'))

producer.flush()