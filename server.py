import asyncio
import json
import logging
import random
import os
import ssl
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

    # Get the connection scheme (ws or wss)
    scheme = "wss" if websocket.url.port == 443 else "ws"
    logging.info(f"🔍 New connection: {scheme}://{websocket.url.hostname}")

    connections.add(websocket)
    logging.info("✅ WebSocket connection established.")

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

    # Load SSL certificates
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_cert = "/app/fullchain.pem"  # Replace with your Render SSL cert path
    ssl_key = "/app/privkey.pem"  # Replace with your Render SSL key path
    if os.path.exists(ssl_cert) and os.path.exists(ssl_key):
        ssl_context.load_cert_chain(ssl_cert, ssl_key)
        logging.info("✅ SSL certificate loaded successfully for WSS.")

    config = uvicorn.Config(app, host="0.0.0.0", port=int(os.environ.get("PORT", 443)), ssl_context=ssl_context)
    server = uvicorn.Server(config)

    await server.serve()  # Start Uvicorn server
    task.cancel()  # Cancel background task on shutdown

if __name__ == "__main__":
    asyncio.run(main())
