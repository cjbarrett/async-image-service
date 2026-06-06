import json
import uuid
from datetime import datetime

import pika
import redis
from fastapi import APIRouter, HTTPException

from models.task_request import TaskRequest
from models.job_status import JobStatus

router = APIRouter()

redis_client = redis.Redis(
    host="redis",
    port=6379,
    decode_responses=True
)


def get_rabbitmq_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters(host="rabbitmq")
    )


@router.post("/tasks")
def create_task(request: TaskRequest):

    job_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()

    job = {
        "job_id": job_id,
        "task": request.task,
        "status": JobStatus.QUEUED.value,
        "created_at": now,
        "updated_at": now
    }

    redis_client.set(job_id, json.dumps(job))

    connection = get_rabbitmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue="task_queue", durable=True)

    message = {
        "job_id": job_id,
        "task": request.task
    }

    channel.basic_publish(
        exchange="",
        routing_key="task_queue",
        body=json.dumps(message)
    )

    connection.close()

    return {
        "job_id": job_id,
        "status": JobStatus.QUEUED.value
    }


@router.get("/tasks/{job_id}")
def get_task_status(job_id: str):

    data = redis_client.get(job_id)

    if not data:
        raise HTTPException(
            status_code=404,
            detail="Job not found"
        )

    return json.loads(data)