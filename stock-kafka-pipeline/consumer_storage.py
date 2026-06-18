from kafka import KafkaConsumer
import mysql.connector
import json
from config import *

conn=mysql.connnector.connect(
	host='MYSQL_HOST',
	user='MYSQL_USER',
	password='MYSQL_PASS',
	database=MYSQL_DB
	)
cursor=conn.cursor()

def deserializer(v):
	return json.loads(v.decode('utf-8'))

consumer=KafkaConsumer(
	KAFKA_TOPIC,
	bootstrap_servers=KAFKA_BROKER,
	group_id='storage-group',
	value_deserializer=deserializer,
	auto_offset_reset='earliest'
	)

print('Storage consumer started. Writing to MYSQL...')

INSERT_SQL="""INSERT INTO stock_events(symbol, ltp, open_price, high_price, low_price, close_price, volume, event_timestamp) VALUES 
			(%s, %s, %s, %s, %s, %s, %s, %s)"""

for message in consumer:
	data=message.value

	values=(
		data['symbol'],
		data['ltp'],
		data['open'],
		data['high'],
		data['low'],
		data['close'],
		data['volume'],
		data['timestamp']
		)		

		try: 
			cursor.execute(INSERT_SQL, values)
			conn.commit()
			print(f"[STORAGE] Saved: {d['symbol']} | LTP={d['ltp:.2f']}")

		except Exception as e:
			print(f"[STORAGE] DB error: {e}")
			conn.rollback()	