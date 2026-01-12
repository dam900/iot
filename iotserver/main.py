import asyncio
import os
import json
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from azure.eventhub.aio import EventHubConsumerClient
from dotenv import load_dotenv

from iotserver.db.models import SessionLocal, engine, get_db, Base, Message

load_dotenv()
logging.basicConfig(level=logging.INFO)

# Konfiguracja Azure Event Hub (Built-in endpoint)
EVENTHUB_CONN_STR = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.getenv("AZURE_EVENTHUB_NAME") # Znajdziesz go w tej samej zakładce co endpoint

Base.metadata.create_all(bind=engine)
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

async def on_event(partition_context, event):
    payload = event.body_as_str()
    # Tworzymy nową sesję dla każdego zdarzenia
    db = SessionLocal() 
    try:
        data = json.loads(payload)
        topic = data.get("topic", "azure/iot")
        content = str(data.get("data", payload))
        
        # 1. Tworzymy i zapisujemy obiekt
        new_msg = Message(topic=topic, payload=content)
        db.add(new_msg)
        db.commit()
        
        # 2. KLUCZOWE: Odświeżamy obiekt, aby dociągnąć wygenerowane ID i datę
        # zanim sesja zostanie zamknięta
        db.refresh(new_msg)
        
        # 3. Przygotowujemy dane do wysyłki (wyciągamy je z obiektu do słownika)
        msg_to_send = {
            "topic": new_msg.topic,
            "payload": new_msg.payload,
            "time": new_msg.created_at.strftime('%Y-%m-%d %H:%M:%S')
        }

        # 4. Wysyłamy przez WebSocket
        await manager.broadcast(msg_to_send)

    except Exception as e:
        print(f"Błąd: {e}")
    finally:
        # Zawsze zamykamy sesję na końcu
        db.close()

async def azure_listener(conn_str, eh_name):
    client = EventHubConsumerClient.from_connection_string(conn_str, "$Default", eventhub_name=eh_name)
    async with client:
        await client.receive(on_event=on_event, starting_position="@latest")

@app.on_event("startup")
async def startup():
    # Pobierz zmienne i uruchom zadanie w tle
    conn = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
    name = os.getenv("AZURE_EVENTHUB_NAME")
    asyncio.create_task(azure_listener(conn, name))

# --- ENDPOINTY ---
@app.get("/", response_class=HTMLResponse)
async def get_index(request: Request, db: Session = Depends(get_db)):
    messages = db.query(Message).order_by(Message.created_at.desc()).limit(50).all()
    return templates.TemplateResponse("index.html", {"request": request, "messages": messages})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)