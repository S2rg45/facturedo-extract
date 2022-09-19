import os
from dotenv import dotenv_values
from sqlalchemy import create_engine
import psycopg2
from pymongo import mongo_client



def connection():
  config = dotenv_values(".env") 
  dictionary_config = dict(config)
  host = dictionary_config["PG_HOST_"]
  port = dictionary_config["PG_PORT_"]
  user = dictionary_config["PG_USERNAME_"]
  password = dictionary_config["PG_PASSWORD_"]
  db = dictionary_config["PG_DB_"]
  conn_string = 'postgresql://{}:{}@{}:{}/{}'.format(user, password,host,port,db)
  db = create_engine(conn_string)
  conn = db.connect()
  connection = psycopg2.connect(conn_string)
  return [connection, conn]



def connection_mongo():
    config = dotenv_values(".env") 
    dictionary_config = dict(config)
    mongo_uri = dictionary_config['URI_DATABASE_MONGO']
    return mongo_client.MongoClient(mongo_uri)