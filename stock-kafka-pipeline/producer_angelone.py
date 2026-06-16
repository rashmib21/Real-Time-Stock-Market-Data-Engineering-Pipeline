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

def get_auth_tokens():
	smart=SmartConnect(api_key=API_KEY)
	totp=pyotp.TOTP(TOTP_SECRET).now()  #generate OTP of 6 digits right before login
	print("6-digit generated OTP: ",totp)
	data=smart.generateSession(CLIENT_ID, PASSWORD, totp) #We send our ID, password, otp to Angel One's Server and the server sends a response with our tokens to authorize us.
	print(data)
	jwt=data['data']['jwtToken'] #Websocket needs this token for authentication and live stock ticks
	# print(jwt)
	feed=smart.getfeedToken() #The feed token is a separate shorter token specifically for the live data WebSocket. Without this token, the WebSocket will reject your connection
	return jwt, feed

def on_open(ws): #ws is the object for websocket, with the help of this object we can subscribe, unsubscribe, or close the connection 
	print("Connected! Subscribing to stock feed..") #It confirms the our application is connected to Angel One's server
	ws.subscribe( #Want live market data for specific stock, this is called subscription
		"session1", #this is a correlation ID which is used as label for the subscription request
		3, #this is subscription mode which gives more information about stock than just the last price
		[{"exchangeType":1,"tokens":[17939]}]) #it defines send me the data of that token number stock for exchange type NSE

def on_data(ws, message): #It automatic execute when the new data arrives, ws: current websocket connection and message is actual data received from Angel one 
    # print("DATA:", message)
    event={
    "symbol":"HINDCOPPER",
    "ltp":message.get("last_traded_price",0)/100,
    "open":message.get("open_price_of_the_day",0)/100,
    "high":message.get("high_price_of_the_day",0)/100,
    "low":message.get("low_price_of_the_day",0)/100,
    "close":message.get("close_price",0)/100,
    "volume":message.get("volume_trade_of_the_day",0)/100,
    "timestamp":message.get("exchange_timestamp")
    }	

    producer.send(KAFKA_TOPIC, key=b 'HINDCOPPER', value=event)
    producer.flush()
    print(f"Sent: {event['symbol']} | LTP={event['ltp']}")


if __name__ == "__main__":
    jwt, feed = get_auth_tokens()
    sws = SmartWebSocketV2(
    auth_token=jwt,
    api_key=API_KEY,
    client_code=CLIENT_ID,
    feed_token=feed
)
    sws.on_open = on_open
    sws.connect()



 