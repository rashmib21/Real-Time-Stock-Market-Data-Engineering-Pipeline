import pyotp
import json
import time
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from kafka import KafkaProducer
from config import *


def json_serializer(value):
	return json.dumps(value).encode('utf-8')  #dumps is used to convert the python objects into json then convert that json into bytes 

producer=KafkaProducer(
	bootstrap_servers=KAFKA_BROKER, #bootstrap means this is the first address the producer connects to Kafka i.e. localhost:9092 
	value_serializer=json_serializer)

sws = None

def get_auth_tokens():
	smart=SmartConnect(api_key=API_KEY)
	totp=pyotp.TOTP(TOTP_SECRET).now()  #generate OTP of 6 digits right before login
	print("6-digit generated OTP: ",totp)
	data=smart.generateSession(CLIENT_ID, PASSWORD, totp) #We send our ID, password, otp to Angel One's Server and the server sends a response with our tokens to authorize us.
	print(data)
	jwt=data['data']['jwtToken'].replace("Bearer ", "") #Websocket needs this token for authentication and live stock ticks
	# print(jwt)
	feed=smart.getfeedToken() #The feed token is a separate shorter token specifically for the live data WebSocket. Without this token, the WebSocket will reject your connection
	return jwt, feed

def on_open(ws): #ws is the object for websocket, with the help of this object we can subscribe, unsubscribe, or close the connection 
	print("Connected! Subscribing to stock feed..") #It confirms the our application is connected to Angel One's server
	sws.subscribe( #Want live market data for specific stock, this is called subscription
		"session1", #this is a correlation ID which is used as label for the subscription request
		3, #this is subscription mode which gives more information about stock than just the last price
		[{"exchangeType":1,"tokens":[17939]}]) #it defines send me the data of that token number stock for exchange type NSE

def on_data(ws, message): #It automatic execute when the new data arrives, ws: current websocket connection and message is actual data received from Angel one 
    # print("DATA:", message)
    event={
    "symbol":"HINDCOPPER",
    "ltp":message.get("last_traded_price",0)/100,  #.get() safely reads a key from the dictionary, 0 is default if the key is missing for any reason, returns 0 instead of crashing
    "open":message.get("open_price_of_the_day",0)/100, #Divide by 100 as Angel One return th value in paisa
    "high":message.get("high_price_of_the_day",0)/100,
    "low":message.get("low_price_of_the_day",0)/100,
    "close":message.get("close_price",0)/100,
    "volume":message.get("volume_trade_of_the_day",0)/100,
    "timestamp":message.get("exchange_timestamp")
    }	

    producer.send(KAFKA_TOPIC, key=b'HINDCOPPER', value=event) #the event dictionary is send to KAFKA_TOPIC is 'stock-ohlcv', key is the message key in Bytes, 
    #kafka uses this key to decide which partition the message goes to, value=event is the actual data - our serializer converts it to bytes automarically.
    #Same key always goes to same partition, keep data ordered
    producer.flush() #flush forces to send everything immediately in the batch to the broker, without flush we have to wait and the message is still in the buffer
    print(f"Sent: {event['symbol']} | LTP={event['ltp']}")


def on_error(ws, error):
	print(f"Error: {error}")

def on_close(ws, *args):
	print("Connection closed! args={args}")


def start():
	global sws
	while True:
		try: 
			jwt, feed=get_auth_tokens()
			sws=SmartWebSocketV2(jwt, API_KEY, CLIENT_ID, feed)
			sws.on_open=on_open
			sws.on_data=on_data
			sws.on_error=on_error
			sws.on_close=on_close
			sws.connect()

		except Exception as e:
			print(f"Disconnected: {e}. Retrying in 5s...")
			time.sleep(5)


if __name__ == "__main__":
#     jwt, feed = get_auth_tokens()
#     sws = SmartWebSocketV2(
#     auth_token=jwt,
#     api_key=API_KEY,
#     client_code=CLIENT_ID,
#     feed_token=feed
# )
#     sws.on_open = on_open
#     sws.connect()

    start()

 