from kafka import KafkaConsumer

# Povezivanje sa Kafka brokerom
consumer = KafkaConsumer('topic3', 
                         bootstrap_servers=['kafka:9092'], 
                         auto_offset_reset='earliest')

# Čitanje poruka sa topic-a
for message in consumer:
    print(message.value.decode())
