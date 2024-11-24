# raw_data_consumer.py

from kafka import KafkaConsumer
from configs import kafka_config
import json

def consume_raw_data():
    consumer = KafkaConsumer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='raw_data_consumer_group'
    )

    my_name = "iuliia"
    topic_building_sensors = f'{my_name}_building_sensors'

    consumer.subscribe([topic_building_sensors])
    print(f"Subscribed to topic '{topic_building_sensors}'")

    try:
        for message in consumer:
            sensor_data = message.value
            print(f"Received raw data: {sensor_data}")
    except KeyboardInterrupt:
        print("\nConsumer stopped manually.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_raw_data()
