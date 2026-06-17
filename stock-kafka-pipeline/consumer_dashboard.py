from kafka import KafkaConsumer
import json
from config import KAFKA_BROKER, KAFKA_TOPIC
from colorama import init, Fore, Style #for making dashboard colorful

def deserializer(v):
	return json.loads(v.decode('utf-8'))
 

consumer=KafkaConsumer(  #This creates consumer object and immediately connects it to Kafka
	KAFKA_TOPIC, #Topic name same as producer topic write in it 
	bootstrap_servers=KAFKA_BROKER, 
	group_id='dashboard-group', #different consumer groups it is of dashboard
	value_deserializer=deserializer,   
	auto_offset_reset='earliest' #position in the topic, earliest means: if the consumer has never run 
	#before and has no saved position, start reading from the very first message which stored in the topic, if we write latest it will read only new messages
		) 

print("Dashboard consumer started. Waiting for data...")
print("-"*50)

for message in consumer: #this loop will run continuously when a new messsage arrives in the topic, Kafka delivered the msg to this loop and it will store in message variable
	data=message.value #message contains several values like key, partition, offset, topic name, we need only values of it
	# print(type(data))
	# print(data['symbol']) #TypeError: byte indices must be integers or slices, not str 

	print(f"Symbol: {data['symbol']}")
	print(f"LTP: Rs.{data['ltp']:2f}")
	print(f"Open: Rs.{data['open']:2f}")
	print(f"High: Rs.{data['high']:2f}")
	print(f"Low: Rs.{data['low']:2f}")
	print(f"Close: Rs.{data['close']:2f}")
	print(f"Volume: Rs.{data['volume']:,}")
	print(f"Time: {data['timestamp']}")
	print("-"*50)	



