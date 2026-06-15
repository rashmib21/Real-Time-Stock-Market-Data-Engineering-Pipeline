import requests
import pandas as pd

url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

print("Downloading scrip master file...")
response = requests.get(url)
scrips = pd.DataFrame(response.json())

result = scrips[scrips['symbol'].str.contains('HINDCOPPER', na=False)]

print(result[['token', 'symbol', 'name', 'exch_seg', 'expiry']].to_string())

#https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json