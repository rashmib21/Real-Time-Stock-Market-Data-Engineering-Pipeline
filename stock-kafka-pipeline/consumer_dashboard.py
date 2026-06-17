from kafka import KafkaConsumer
import json
from config import KAFKA_BROKER, KAFKA_TOPIC
from colorama import init, Fore, Style #for making dashboard colorful

init(autoreset=True) #resets color after every print automatically

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

print(Fore.CYAN+"Dashboard consumer started. Waiting for data..."+Style.RESET_ALL) #Fore: foreground colors
print(Fore.CYAN+"-"*50+Style.RESET_ALL)

for message in consumer: #this loop will run continuously when a new messsage arrives in the topic, Kafka delivered the msg to this loop and it will store in message variable
	data=message.value #message contains several values like key, partition, offset, topic name, we need only values of it
	# print(type(data))
	# print(data['symbol']) #TypeError: byte indices must be integers or slices, not str 

	print(Fore.YELLOW + f"Symbol : {data['symbol']}" + Style.RESET_ALL)
	print(Fore.GREEN  + f"LTP    : Rs. {data['ltp']:.2f}" + Style.RESET_ALL)
	print(Fore.WHITE  + f"Open   : Rs. {data['open']:.2f}" + Style.RESET_ALL)
	print(Fore.WHITE  + f"High   : Rs. {data['high']:.2f}" + Style.RESET_ALL)
	print(Fore.WHITE  + f"Low    : Rs. {data['low']:.2f}" + Style.RESET_ALL)
	print(Fore.WHITE  + f"Close  : Rs. {data['close']:.2f}" + Style.RESET_ALL)
	print(Fore.MAGENTA+ f"Volume : {data['volume']:,}" + Style.RESET_ALL)
	print(Fore.BLUE   + f"Time   : {data['timestamp']}" + Style.RESET_ALL)
	print(Fore.CYAN   + "-" * 50 + Style.RESET_ALL)

	



