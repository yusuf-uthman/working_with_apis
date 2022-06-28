import requests
import pandas as pd


#get pages
url = 'https://rickandmortyapi.com/api/character?page=1'
response = requests.get(url)
if response.status_code == 200:
    pages = response.json()['info']['pages']

page = 1
data_pages = []
while page <= pages:
    url = f'https://rickandmortyapi.com/api/character?page={page}'
    response = requests.get(url)
    if response.status_code == 200:
        response = response.json()
        data_pages.extend(response['results'])
        page += 1

print(f"lenght of data is {len(data_pages)}")

df_pages = pd.DataFrame(data_pages)

print(df_pages.head())