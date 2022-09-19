import psycopg2
import pandas as pd
from datetime import datetime
import json

from app.connection.db import *
from dotenv import dotenv_values
from bson.objectid import ObjectId
import pydantic
pydantic.json.ENCODERS_BY_TYPE[ObjectId]=str



def transform_data(item):
    try:
        validate = validate_id_deudor(item)
        if len(validate) > 0:
            return validate
        else:    
            config = dotenv_values(".env") 
            dictionary_config = dict(config)
            name_table = dictionary_config["NAME_TABLE"]
            name_schema = dictionary_config["NAME_SCHEMA"]
            engine = connection()
            count_deudor = validate_id_deudores(engine, name_table, name_schema, item)
            if len(count_deudor) > 0:
                count_ope = count_operation(engine, name_table, name_schema, item)
                count_umbral = umbrales_outlir(engine, name_table, name_schema, item)
                count_volumen = max_volumen(engine, name_table, name_schema, item)
                count_list = list_operation(engine, name_table, name_schema, item ,count_umbral)
                count_average = six_month_average(engine, name_table, name_schema, item)
                count_null = null_values(engine, name_table, name_schema, item)
                data = {"id_deudor": item.id_deudor, "information": [count_ope,count_umbral, count_volumen, count_list, count_average, count_null]}
                client = connection_mongo()
                database = client.facturedo
                create_one = database.id_deudor.insert_one(data)
                return json.JSONEncoder().encode(create_one)   
            else: 
                return {"messagess": "Este id no se encuentra"}
                
    except OSError as error:
        return {"messages": "No se puede leer parametro"} 
      
    

def validate_id_deudores(engine, name_table, name_schema, item):
    connection_db = engine
    connection_db[0].autocommit = True
    cursor = connection_db[0].cursor()
    sql_count = """
                select * 
                from {}.{} f 
                where "id deudor" = {}
                """.format(name_schema, name_table, item.id_deudor)
    cursor.execute(sql_count)
    data_count = cursor.fetchall()
    return data_count
        
        

def count_operation(engine, name_table, name_schema, item):
    connection_db = engine
    connection_db[0].autocommit = True
    cursor = connection_db[0].cursor()
    sql_count = """
                select 
                        "result",
                        count("result") 
                from {}.{} 
                where "id deudor" = {}
                group by "result", "id deudor"
                """.format(name_schema, name_table, item.id_deudor)
    cursor.execute(sql_count)
    data_count = cursor.fetchall()
    dictionary_result = {}
    for item in data_count:
        dictionary_result[item[0]] = item[1]
    return {"conteo_operaciones": dictionary_result}



def umbrales_outlir(engine, name_table, name_schema, item):
    connection_db = engine
    connection_db[0].autocommit = True
    cursor = connection_db[0].cursor()
    sql_count = """
                select 
                        amount
                from {}.{} 
                where "id deudor" = {}
                """.format(name_schema, name_table, item.id_deudor)
    cursor.execute(sql_count)
    data_count = cursor.fetchall()
    dataframe = pd.DataFrame(data_count, columns=['amount'])
    describe_data = dataframe.describe()
    dict_data = list(describe_data.to_dict('records'))
    percentage_min = list(dict_data[4].values())
    percentage_max = list(dict_data[6].values())
    percentage_half = percentage_max[0] - percentage_min[0]
    value_min = percentage_min[0] - 1.5 * percentage_half
    value_max = percentage_max[0] + 1.5 * percentage_half
    dict_umbral = {"umbral_inf": value_min, "umbral_superior": value_max}
    return  {"umbrales_outliers": dict_umbral}
    


def max_volumen(engine, name_table, name_schema, item):
    connection_db = engine
    connection_db[0].autocommit = True
    cursor = connection_db[0].cursor()
    sql_count = """
                select  payment_method,
                        max(anual_sales_volume)
                from {}.{} f 
                where "id deudor" = {}
                group by payment_method 
                """.format(name_schema, name_table, item.id_deudor)
    cursor.execute(sql_count)
    data_count = cursor.fetchall()
    dictionary_result = {}
    for item in data_count:
        dictionary_result[item[0]] = item[1]
    return {"tipo_pago_max_volumen": dictionary_result}  


def list_operation(engine, name_table, name_schema, item, operations):
    umbral_sup = operations['umbrales_outliers']['umbral_superior']
    connection_db = engine
    connection_db[0].autocommit = True
    cursor = connection_db[0].cursor()
    sql_count = """
                select operation_id,
                        amount
                from {}.{} f 
                where "id deudor" = {} 
                and amount >= {}
                """.format(name_schema, name_table, item.id_deudor, umbral_sup)
    cursor.execute(sql_count)
    data_count = cursor.fetchall() 
    dictionary_result = {}
    if len(data_count) > 0:
        for item in data_count:
            dictionary_result[item[0]] = item[1]
        return {"lista_operaciones": dictionary_result}
    else:
        return {"lista_operaciones": ""}
      
      
      
def six_month_average(engine, name_table, name_schema, item):
    connection_db = engine
    connection_db[0].autocommit = True
    cursor = connection_db[0].cursor()
    sql_count = """
                select 
                  avg(max_amount) 
                from (
                    select 
                      creation_dates as create ,
                      max(amount) as max_amount
                    from(
                        select to_char(cast(creation_date as date), 'YYYY-MM') as creation_dates,
                            amount as amount
                        from {}.{} f 
                        where "id deudor" = {} 
                        and  cast (creation_date as date) between cast(current_date - interval '6 month' as date) and current_date
                    ) as d 
                    group by creation_dates
                ) as f	
                """.format(name_schema, name_table, item.id_deudor)
    cursor.execute(sql_count)
    data_count = cursor.fetchall() 
    return {"prom_max_ult_6_meses": round(data_count[0][0])}
  
  

def null_values(engine, name_table, name_schema, item):
    date_now = datetime.now()
    format_date = date_now.strftime('%Y-%m-%d')
    connection_db = engine
    sql_count = """
                select *
                from {}.{} f 
                where "id deudor" = {} 
                """.format(name_schema, name_table, item.id_deudor)
    data_count = pd.read_sql_query(sql_count, connection_db[1])
    data_is_null = data_count.isnull().sum()
    dictionary_data_null = data_is_null.to_dict()
    column_data_null = [key for key, value in dictionary_data_null.items() if value > 0]
    value_null = {format_date:item for item in column_data_null}
    return {"valores_nulos": value_null}

  
def validate_id_deudor(item):
    client = connection_mongo()
    database = client.facturedo
    data = database.id_deudor.find({'id_deudor': item.id_deudor})
    list_deudor = [doc for doc in data]
    return list_deudor

    
 