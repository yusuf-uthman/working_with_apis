import requests
from   sqlalchemy import create_engine
import os
# import config

# api page start
page_num = 41

# call database credentials from imported config file
# password  = config.password
# user      = config.user
# db        = config.db
# port      = config.port
# host      = config.host
# table     = 'rick_and_morty'
# schema    = 'api_schema'

# credentials and variables from 
port     = int(os.environ.get("HEROKU_DEMO_PG_PORT"))
schema   = os.environ.get("HEROKU_DEMO_PG_SCHEMA")
table    = os.environ.get("HEROKU_DEMO_PG_TABLE")
password = os.environ.get("HEROKU_DEMO_PG_PASS")
user     = os.environ.get("HEROKU_DEMO_PG_USER")
db       = os.environ.get("HEROKU_DEMO_PG_DB")
host     = os.environ.get("HEROKU_DEMO_PG_HOST")


# create connetion to database
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
conn   = engine.raw_connection()
cursor = conn.cursor()

# main function that calls all other functions
def process_api_data_all(schema, table, page_num):
    create_schema(schema)
    create_table(schema, table)
    truncate_table(schema, table)
    data_inserted = pull_and_insert_data(schema, table, page_num)
    data_fetched = query_database(schema, table)
    if len(data_inserted[1]) == len(data_fetched):
        print(f"""Data count from api and database are {len(data_inserted[1])} and {len(data_fetched)} respectively. Operation successfull!""")
    else:
        print("""Incomplete data transfer!""")        
    conn.close()

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

# truncate table function #This function is not required if we are doing incremental load or change data capture(CDC)
def truncate_table(schema, table):
    cursor.execute(f""" Truncate table {schema}.{table}; """)
    conn.commit()      

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

# fetch inserted data from database
def query_database(schema, table):
    query_result = []    
    script  = f"""SELECT * FROM {schema}.{table};"""
    cursor.execute(script)
    for rec in cursor.fetchall():
        query_result.append(rec)
    return query_result

# # call main function to run the program
# if __name__ == '__main__':
#     schema   = input("Enter Schema Name: ").lower()
#     table    = input("Enter Table Name: ").lower()
#     page_num = int(input("Enter API Start Page Number: "))
#     process_api_data_all(schema, table, page_num)

# call main function to run the program
if __name__ == '__main__':
    process_api_data_all(schema, table, page_num)
