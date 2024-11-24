from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',  # Read messages from the beginning
    enable_auto_commit=True,       # Automatically commit offsets
    group_id='sensor_alerts_consumer_group'
)

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

my_name = "iuliia"
topic_building_sensors = f'{my_name}_building_sensors'
topic_temperature_alerts = f'{my_name}_temperature_alerts'
topic_humidity_alerts = f'{my_name}_humidity_alerts'

# Subscribe to input topic
consumer.subscribe([topic_building_sensors])

print(f"Subscribed to topic '{topic_building_sensors}'")

# Process messages and generate alerts
try:
    for message in consumer:
        sensor_data = message.value

        # print(f"Received raw data: {sensor_data}")

        sensor_id = sensor_data.get("sensor_id")
        timestamp = sensor_data.get("timestamp")
        temperature = sensor_data.get("temperature")
        humidity = sensor_data.get("humidity")

        # Check for temperature alerts
        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "message": f"High temperature alert! Detected temperature: {temperature}°C."
            }
            producer.send(topic_temperature_alerts, key=sensor_id, value=alert)
            print(f"Temperature alert sent: {alert}")

        # Check for humidity alerts
        if humidity > 80 or humidity < 20:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "humidity": humidity,
                "message": f"Humidity alert! Detected humidity: {humidity}%."
            }
            producer.send(topic_humidity_alerts, key=sensor_id, value=alert)
            print(f"Humidity alert sent: {alert}")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
    producer.close()
