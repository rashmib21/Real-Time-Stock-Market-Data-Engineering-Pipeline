from kafka import KafkaConsumer
from collections import deque
import json
from config import KAFKA_BROKER, KAFKA_TOPIC

def deserializer(v):
	return json.loads(v.decode('utf-8'))

consumer=KafkaConsumer(
	KAFKA_TOPIC,
	bootstrap_servers=KAFKA_BROKER,
	group_id='analytics-group',
	value_deserializer=deserializer,
	auto_offset_reset='earliest'
	)
price_window=deque(maxlen=5)