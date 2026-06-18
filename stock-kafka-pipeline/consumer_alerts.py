from kafka import KafkaConsumer
import json
from config import *

def deserializer(v):
	return json.loads(v.decode('utf-8'))

consumer=KafkaConsumer(
	KAFKA_TOPIC,
	bootstrap_servers=KAFKA_BROKER,
	group_id='alerts-group',
	value_deserializer=deserializer,
	auto_offset_reset='earliest'
	)