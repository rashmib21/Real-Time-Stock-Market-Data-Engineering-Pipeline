from kafka import KafkaConsumer
from collections import deque  #deque is a special list which removes the old item when it gets full
import json
from config import KAFKA_BROKER, KAFKA_TOPIC

def deserializer(v):
	return json.loads(v.decode('utf-8'))

consumer=KafkaConsumer(
	KAFKA_TOPIC,
	bootstrap_servers=KAFKA_BROKER,
	group_id='analytics-group', #different group id for analytics instead of dashboard
	value_deserializer=deserializer,
	auto_offset_reset='earliest'
	)
price_window=deque(maxlen=5) #it creates an empty deque which can hold at most 5 items. When the 6th item is added the first old one is removed 


print("Analytics consumer started...")
for message in consumer:
	data=message.value
	close=data['close']

	