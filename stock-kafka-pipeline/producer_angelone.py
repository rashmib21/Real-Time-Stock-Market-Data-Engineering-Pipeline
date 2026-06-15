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




if __name__ == "__main__":
    jwt, feed = get_auth_tokens()


    
    # print(jwt)
    # print(feed)
