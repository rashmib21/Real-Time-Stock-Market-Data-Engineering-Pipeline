from kafka import KafkaConsumer
import json
from config import KAFKA_BROKER, KAFKA_TOPIC

def deserializer(v):
	return json.loads(v.decode('utf-8'))
 

consumer=KafkaConsumer(
	KAFKA_TOPIC,
	bootstrap_servers=KAFKA_BROKER,
	group_id='dashboard-group',
	auto_offset_reset='earliest'
	) 