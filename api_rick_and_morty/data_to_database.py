import requests
import pandas     as pd
from   sqlalchemy import create_engine 
import config

password  = config.password
user      = config.user
db        = config.db
port      = config.port
host      = config.host

# create connetion
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
conn   = engine.raw_connection()
cursor = conn.cursor()

print(engine)