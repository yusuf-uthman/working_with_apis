import requests
import pandas as pd

url = 'https://rickandmortyapi.com/api/character?page=1'
data_next_page = []
while url:
#     print(f'Requesting,  {url}')
    response = requests.get(url)
    if response.status_code == 200:
        response = response.json()
        data_next_page.extend(response['results'])
        url = response['info']['next']

print(f"The count of records in the file is {data_next_page}")

df_data_next_page = pd.DataFrame(data_next_page)

print(df_data_next_page.head())