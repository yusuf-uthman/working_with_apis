import requests
import pandas     as pd
from   sqlalchemy import create_engine 
import config

password  = config.password
user      = config.user
db        = config.db
port      = config.port
host      = config.host


# create connetion to database
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
conn   = engine.raw_connection()
cursor = conn.cursor()


# pull data from the api into alist as both list and dictionary
page_num = 40 #This proces start from 40 to avoid multiple unneccessary to the API

actor_dict_list = []
actor_list_list = []

while True:
    url = f'https://rickandmortyapi.com/api/character?page={page_num}'
    response = requests.get(url)
    data  = response.json()
    
    try:
        print(len(data['results'][0]))
    except:
        print('No more results, Exit loop')
        break
    for rec in data['results']:
        #dictionary
        actor_dict ={
            'id': rec['id'],
            'name': rec['name'],
            'species': rec['species'],
            'type': rec['type'],
            'gender': rec['gender'],
            'origin': rec['origin']['name'],
            'url': rec['url'],
            'location': rec['location']['name'],
            'image': rec['image'],
            'num_episode_feature': len(rec['episode']),
            'created': rec['created']
        }
        #list
        actor_list =[
           rec['id'],
           rec['name'],
           rec['species'],
           rec['type'],
           rec['gender'],
           rec['origin']['name'],
           rec['url'],
           rec['location']['name'],
           rec['image'],
       len(rec['episode']),
           rec['created']
        ]
        actor_dict_list.append(actor_dict)
        actor_list_list.append(actor_list)   
    page_num +=1

print(f"Actor_Dict_List has {len(actor_dict_list)} and Actor_List_List len(actor_list_list)")

print(engine)

