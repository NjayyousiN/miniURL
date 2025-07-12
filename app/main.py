from fastapi import FastAPI, Depends
from database.connect import get_cassandra_session

app = FastAPI()


@app.get("/")
async def read_root():
    return {"message": "Welcome to the miniURL API!"}
