from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from bson.objectid import ObjectId
import pydantic
pydantic.json.ENCODERS_BY_TYPE[ObjectId]=str

from app.model.items import Items
from app.transform.index import transform_data

items = Items

app = FastAPI()

origins = []


app.add_middleware(
	 CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/api/transform/facturedo")
async def main(item: Items):
    try:
        return transform_data(item)
    except OSError as err:
        return {"messages": "Datos enviados en el body fallaron" }
 
 


  
