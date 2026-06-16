from kafka import KafkaConsumer
import json
from config import KAFKA_BROKER, KAFKA_TOPIC
 

consumer=KafkaConsumer(
	KAFKA_TOPIC,
	bootstrap_servers=KAFKA_BROKER,
	group_id='dashboard-group',
	) 