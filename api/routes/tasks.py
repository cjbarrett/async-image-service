import json
import uuid

import pika
from fastapi import APIRouter, HTTPException

from models.task_request import TaskRequest
from models.job_status import JobStatus

router = APIRouter()

# temporary in-memory job store
jobs = {}


def get_rabbitmq_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters(host="rabbitmq")
    )


@router.post("/tasks")
def create_task(request: TaskRequest):

    job_id = str(uuid.uuid4())

    # store initial job state
    jobs[job_id] = {
        "status": JobStatus.QUEUED,
        "task": request.task
    }

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

    job = jobs.get(job_id)

    if not job:
        raise HTTPException(
            status_code=404,
            detail="Job not found"
        )

    return {
        "job_id": job_id,
        "status": job["status"].value
    }