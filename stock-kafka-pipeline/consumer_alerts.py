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

print(f"Alerts consumer started. Alter threshold: Rs. {ALERT_PRICE}")

last_alert_state=None #this variable remembers whether the price was above or below the threshold in the previous tick

for message in consumer:
	data=message.value
	ltp=data['ltp']
	symbol=data['symbol']