import asyncio
import json
import logging
import random
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()
connections = set()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Handles WebSocket connections."""
    await websocket.accept()
    connections.add(websocket)
    logging.info("✅ New WebSocket connection established.")

    try:
        while True:
            message = await websocket.receive_text()
            logging.info(f"📩 Received message: {message}")
    except WebSocketDisconnect:
        connections.remove(websocket)
        logging.warning("⚠️ WebSocket connection closed.")

async def send_random_events():
    """Sends random data to all connected clients every 15 seconds."""
    while True:
        if connections:
            event = {
                "timestamp": asyncio.get_event_loop().time(),
                "customer_id": str(random.randint(10000, 99999)),
                "event_type": random.choice(["pickup", "putback", "exit"]),
                "item": {
                    "name": random.choice(["Milk", "Bread", "Eggs", "Cheese", "Chicken"]),
                    "barcode": str(random.randint(1000000000, 9999999999)),
                    "weight": f"{random.randint(1, 5)}kg"
                }
            }
            event_json = json.dumps(event)
            logging.info(f"📤 Sending event: {event_json}")

            for websocket in list(connections):
                try:
                    await websocket.send_text(event_json)
                except:
                    connections.remove(websocket)

        await asyncio.sleep(15)

async def main():
    """Runs the WebSocket server and background tasks."""
    task = asyncio.create_task(send_random_events())  # Start background event loop
    config = uvicorn.Config(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    server = uvicorn.Server(config)

    await server.serve()  # Start Uvicorn server
    task.cancel()  # Cancel background task on shutdown

if __name__ == "__main__":
    asyncio.run(main())
