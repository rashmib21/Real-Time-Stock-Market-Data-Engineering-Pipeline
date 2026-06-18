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