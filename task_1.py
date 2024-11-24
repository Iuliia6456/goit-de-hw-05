from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення нових топіків
my_name = "iuliia"
topic_building_sensors = f'{my_name}_building_sensors'
topic_temperature_alerts = f'{my_name}_temperature_alerts'
topic_humidity_alerts = f'{my_name}_humidity_alerts'

num_partitions = 2
replication_factor = 1

# Створення нових топіків
new_topics = [
    NewTopic(name=topic_building_sensors, num_partitions=num_partitions, replication_factor=replication_factor),
    NewTopic(name=topic_temperature_alerts, num_partitions=num_partitions, replication_factor=replication_factor),
    NewTopic(name=topic_humidity_alerts, num_partitions=num_partitions, replication_factor=replication_factor)
]

# Створення нових топіків
try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"Topics '{', '.join([topic.name for topic in new_topics])}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків
[print(topic) for topic in admin_client.list_topics() if "my_name" in topic]

# Закриття зв'язку з клієнтом
admin_client.close()
