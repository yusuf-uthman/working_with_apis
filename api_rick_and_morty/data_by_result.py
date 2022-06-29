import requests
import pandas as pd

page_num = 40
data_results = []

while True:
    url = f'https://rickandmortyapi.com/api/character?page={page_num}'
    response = requests.get(url)
    data  = response.json()
    
    try:
        print(len(data['results'][0]))
    except:
        print('No more results, Exit loop')
        break
    data_results.extend(data['results'])    
    page_num +=1

print(f"lenght of data is {data_results}")

df_data_results = pd.DataFrame(data_results)
print(df_data_results.head())