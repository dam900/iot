from fastapi import FastAPI
from fastapi import Depends
from sqlalchemy.orm import Session

from iotserver.db.models import get_db

app = FastAPI()

@app.get("/")
async def root(db: Session = Depends(get_db)):
    return {"message": "Hello World"}
