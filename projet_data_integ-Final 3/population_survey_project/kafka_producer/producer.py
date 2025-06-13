from kafka import KafkaProducer
import json
import time
import csv
import os

KAFKA_BROKER = 'localhost:9093'
TOPIC = 'population_data'

# Création du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def read_population_data():
    data = []
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(BASE_DIR, 'total-population.csv'), 'r') as csvfile:
    #with open('total-population.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data

def send_to_kafka():
    """Envoie les données à Kafka une par une."""
    while True:
        data_batch = read_population_data()
        for record in data_batch:
            producer.send(TOPIC, value=record)
            print(f"Sent record: {record}")
        time.sleep(10)           

if __name__ == "__main__":
    send_to_kafka()
