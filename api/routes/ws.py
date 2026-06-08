import json
import asyncio
import redis
from fastapi import APIRouter, WebSocket
from deps import redis_client

from models.protocol import WSMessageType, WSMessageMode
from config import REDIS_HOST, REDIS_PORT, STREAM_KEY, GROUP, CONSUMER

router = APIRouter()

# Ensure consumer group exists
try:
    redis_client.xgroup_create(
        STREAM_KEY,
        GROUP,
        id="0",
        mkstream=True
    )
except redis.exceptions.ResponseError:
    pass


@router.websocket("/ws/jobs")
async def job_updates(websocket: WebSocket):
    await websocket.accept()

    await websocket.send_json({
        "type": WSMessageType.CONNECTION.value,
        "status": "connected"
    })

    # --------------------------------------------------
    # 1. REPLAY OLD EVENTS (the "history" part)
    # --------------------------------------------------
    try:
        history = redis_client.xrange(STREAM_KEY, min="-", max="+", count=50)

        for event_id, fields in history:
            await websocket.send_json({
                "type": "job_update",
                "mode": "replay",
                "event_id": event_id,
                "payload": fields
            })
    except Exception as e:
        print(f"[WS] replay error: {e}")

    # --------------------------------------------------
    # 2. LIVE STREAM (tail -f part)
    # --------------------------------------------------
    while True:
        try:
            messages = redis_client.xreadgroup(
                GROUP,
                CONSUMER,
                {STREAM_KEY: ">"},
                count=10,
                block=5000
            )

            if messages:
                for stream, events in messages:
                    for event_id, fields in events:

                        await websocket.send_json({
                            "type": "job_update",
                            "mode": "live",
                            "event_id": event_id,
                            "payload": fields
                        })

                        redis_client.xack(STREAM_KEY, GROUP, event_id)

            await asyncio.sleep(0.05)

        except Exception as e:
            print(f"[WS] live stream error: {e}")
            break

    try:
        await websocket.close()
    except:
        pass