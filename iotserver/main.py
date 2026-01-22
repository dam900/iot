from datetime import datetime
from http.client import HTTPException
from pydantic import BaseModel
import asyncio
import os
import json
import logging
import anyio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
# from sqlalchemy.orm import Session
from azure.eventhub.aio import EventHubConsumerClient
from azure.iot.hub import IoTHubRegistryManager

from dotenv import load_dotenv

# from iotserver.db.models import SessionLocal, engine, get_db, Base, Message

load_dotenv()
logging.basicConfig(level=logging.INFO)

# Konfiguracja Azure Event Hub (Built-in endpoint)
EVENTHUB_CONN_STR = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.getenv("AZURE_EVENTHUB_NAME") # Znajdziesz go w tej samej zakładce co endpoint

IOTHUB_SERVICE_CONN_STR = os.getenv("IOTHUB_SERVICE_CONNECTION_STRING")
DEVICE_ID = os.getenv("IOTHUB_DEVICE_ID")

# Base.metadata.create_all(bind=engine)
app = FastAPI()
templates = Jinja2Templates(directory="templates")
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass # Ignoruj martwe połączenia

manager = ConnectionManager()
registry_manager = None

class Message:

    def __init__(self, topic, payload):
        self.id = 1
        self.topic = topic
        self.payload = payload
        self.created_at = datetime.now()

async def on_event(partition_context, event):
    payload = event.body_as_str()
    # db = SessionLocal() 
    try:
        # Próba sparsowania JSON, ale z fallbackiem
        try:
            data = json.loads(payload)
            topic = data.get("topic", "azure/iot-device")
            # Jeśli data to słownik, używamy go, jeśli nie - całego payloadu
            content = data.get("data", payload) 
        except json.JSONDecodeError:
            topic = "azure/raw-data"
            content = payload

        # Konwersja contentu na string dla bazy danych
        str_content = json.dumps(content) if isinstance(content, dict) else str(content)

        new_msg = Message(topic=topic, payload=str_content)
        # db.add(new_msg)
        # db.commit()
        # db.refresh(new_msg)
        
        msg_to_send = {
            "topic": new_msg.topic,
            "payload": new_msg.payload,
            "time": new_msg.created_at.strftime('%Y-%m-%d %H:%M:%S')
        }

        logging.info(f"Broadcasting: {msg_to_send}")
        await manager.broadcast(msg_to_send)

    except Exception as e:
        logging.error(f"Krytyczny błąd on_event: {e}")
    finally:
        pass
        # db.close()

async def azure_listener(conn_str, eh_name):
    client = EventHubConsumerClient.from_connection_string(conn_str, "$Default", eventhub_name=eh_name)
    async with client:
        await client.receive(on_event=on_event, starting_position="@latest")


class Telemetry(BaseModel):
    topic: str
    payload: str


@app.on_event("startup")
async def startup():
    global registry_manager
    conn = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
    name = os.getenv("AZURE_EVENTHUB_NAME")
    
    # Inicjalizacja producenta raz na całą sesję
    registry_manager = IoTHubRegistryManager(IOTHUB_SERVICE_CONN_STR)
    
    # Uruchomienie listenera w tle
    asyncio.create_task(azure_listener(conn, name))

@app.on_event("shutdown")
async def shutdown():
    global registry_manager
    if registry_manager:
        await registry_manager.close()

# --- ENDPOINTY ---
@app.get("/", response_class=HTMLResponse)
async def get_index(request: Request):
# async def get_index(request: Request, db: Session = Depends(get_db)):
    # messages = db.query(Message).order_by(Message.created_at.desc()).limit(50).all()
    messages = []
    return templates.TemplateResponse("index.html", {"request": request, "messages": messages})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.post("/send")
async def send_to_device(data: Telemetry):
    if not registry_manager:
        raise HTTPException(status_code=500, detail="Registry Manager not initialized")
    
    loop = asyncio.get_event_loop()
    try:
        # Przygotowanie body
        message_body = json.dumps({"topic": data.topic, "data": data.payload})
        
        await anyio.to_thread.run_sync(
            registry_manager.send_c2d_message, 
            DEVICE_ID, 
            message_body
        )
        
        return {"status": "sent"}
    except Exception as e:
        logging.error(f"Send error: {e}")
        raise HTTPException(status_code=500, detail=str(e))