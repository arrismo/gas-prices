import requests
import pandas as pd
import json




response = requests.get('https://newsapi.org/v2/everything?q=technology&apiKey=afd67a6c7063461fa98d390ac2b40500')
print(response.json())

