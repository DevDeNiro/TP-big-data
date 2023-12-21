from kafka import KafkaConsumer
import json

from kafka.errors import KafkaError

# Kafka Consumer configuration
consumer = KafkaConsumer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

consumer.subscribe(['tp-big-data'])

while True:
    try:
        records = consumer.poll(timeout_ms=1000)

        if not records:
            print("Aucun message à récupérer en ce moment.")
        else:
            for partition, record_list in records.items():
                for record in record_list:
                    message = record.value
                    print(f'Partition: {partition}, Message: {message}')

    except KafkaError as e:
        print(f"Erreur Kafka: {e}")
    except Exception as e:
        print(f"Erreur inattendue: {e}")
