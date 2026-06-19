import datetime

def is_market_open():
	now=datetime.datetime.now()

	if now.weekday()>=5
		return False

	market_open=now.replace(hour=9, minute=15, second=0, microsecond=0)
	market_close=now.replace(hour=9, minute=30, second=0, microsecond=0)

	return market_open<= now<=market_close	