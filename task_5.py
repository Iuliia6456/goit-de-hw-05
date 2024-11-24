from kafka import KafkaConsumer
from configs import kafka_config
import json

# Create Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='alerts_consumer_group_1'
)

# Topics to subscribe
my_name = "iuliia"
topic_temperature_alerts = f'{my_name}_temperature_alerts'
topic_humidity_alerts = f'{my_name}_humidity_alerts'

consumer.subscribe([topic_temperature_alerts, topic_humidity_alerts])

print(f"Subscribed to topics: {topic_temperature_alerts}, {topic_humidity_alerts}")

# Process notifications
try:
    for message in consumer:
        alert = message.value  # alert message
        topic = message.topic  # topic
        print(f"\nAlert received from topic '{topic}':")
        print(json.dumps(alert, indent=4, ensure_ascii=False))

except KeyboardInterrupt:
    print("\nConsumer stopped manually.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
