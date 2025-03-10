import asyncio
import json
import logging
import os
import random
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn
from pulsar import Client, ConsumerType

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Pulsar Configuration
PULSAR_BROKER = "pulsar://localhost:6650"
TOPICS = ["customer-12345", "customer-67890", "customer-98765"]  # Dynamic topics

# FastAPI App
app = FastAPI()
connections = {}  # Stores active WebSocket connections

@app.websocket("/ws/{customer_id}")
async def websocket_endpoint(websocket: WebSocket, customer_id: str):
    """Handles WebSocket connections per customer ID."""
    await websocket.accept()
    if customer_id not in connections:
        connections[customer_id] = set()
    connections[customer_id].add(websocket)
    logging.info(f"✅ WebSocket connected for customer {customer_id}")

    try:
        while True:
            message = await websocket.receive_text()
            logging.info(f"📩 Received message from {customer_id}: {message}")
    except WebSocketDisconnect:
        connections[customer_id].remove(websocket)
        if not connections[customer_id]:  # Remove empty sets
            del connections[customer_id]
        logging.warning(f"⚠️ WebSocket disconnected for customer {customer_id}")

async def pulsar_consumer():
    """Consumes messages from Pulsar and sends them to WebSocket clients."""
    client = Client(PULSAR_BROKER)
    consumers = {topic: client.subscribe(topic, "websocket-server", consumer_type=ConsumerType.Shared) for topic in TOPICS}

    while True:
        for topic, consumer in consumers.items():
            try:
                msg = consumer.receive(timeout_millis=500)
                event = json.loads(msg.data())
                logging.info(f"🔔 Pulsar event received: {event}")

                customer_id = event.get("customer_id")
                if customer_id in connections:
                    for websocket in list(connections[customer_id]):
                        try:
                            await websocket.send_text(json.dumps(event))
                        except:
                            connections[customer_id].remove(websocket)

                consumer.acknowledge(msg)
            except Exception as e:
                continue  # Ignore empty queue exceptions

async def main():
    """Runs the WebSocket server & Pulsar consumer."""
    pulsar_task = asyncio.create_task(pulsar_consumer())  # Start Pulsar consumer
    config = uvicorn.Config(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    server = uvicorn.Server(config)

    await server.serve()
    pulsar_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
