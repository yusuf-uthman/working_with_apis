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


# function to pull and insert api data inot the database
def pull_and_insert_data(schema, table, page_num):
    actor_dict_list = []  #list for dicts
    actor_list_list = []  #list for lists

    while True:
        url = f'https://rickandmortyapi.com/api/character?page={page_num}'
        response = requests.get(url)
        data  = response.json()
        # try block to catch errors when there is no more data from the dapi
        try:
            print(f"This payload has {len(data['results'][0])} records")
        except:
            print('No more data in API, Exited loop!')
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
    for rec in actor_list_list:
        cursor.execute(f"""INSERT into {schema}.{table}(id, name, species, type, gender, origin, url, location,
                           image, num_episode_feature, created) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)  
                        """, rec)
        conn.commit()
    return[actor_dict_list,actor_list_list]

# create schema function
def create_schema(schema):
    cursor.execute(f""" CREATE SCHEMA  if not exists {schema};""")
    conn.commit()
    
# create table function
def create_table(schema, table):
    cursor.execute(f""" 
    CREATE TABLE IF NOT EXISTS {schema}.{table}(
                        id SERIAL primary key,
                        name varchar(200),
                        species varchar(200),
                        type varchar(200),
                        gender varchar(20),
                        origin varchar(200),
                        url varchar(200),
                        location varchar(200),
                        image varchar(200),
                        num_episode_feature int,
                        created timestamp,  
                        insert_date date DEFAULT now()
    )
    """)
    conn.commit()  

# truncate table function
def truncate_table(schema, table):
    cursor.execute(f""" Truncate table {schema}.{table}; """)
    conn.commit()      


# fetch inserted data from database
def query_database(schema, table):
    query_result = []    
    script  = f"""SELECT * FROM {schema}.{table};"""
    cursor.execute(script)
    for rec in cursor.fetchall():
        query_result.append(rec)
    return query_result

# View db records using pandas  
import pandas as pd
column_names=['id', 'name', 'species', 'type', 'gender', 'origin', 'url', 'location', 'image', 'num_episode_feature',
              'created', 'insert_date']
df = pd.DataFrame(query_result, columns = column_names)
print(df.head())