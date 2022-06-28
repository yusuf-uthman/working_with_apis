import requests
import pandas as pd

page = 1
data_count = []
while True:
    url = f'https://rickandmortyapi.com/api/character?page={page}'
#     print(f'Requesting,  {url}')
    response = requests.get(url)
    if response.status_code == 200 and len(data_count) < response.json()['info']['count']:
#         print(len(data_count))
        data = response.json()
        data_count.extend(data['results'])
        page += 1
    else:
        break

print(f"lenght of data is {len(data_count)}")

df_data_count = pd.DataFrame(data_count)

print(df_data_count.head())