from fastapi import APIRouter
import uuid
import json
import pika

from models.job import TaskCreate
from services.job_store import create_job
from models.job_status import JobStatus

router = APIRouter()


def get_channel():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters("rabbitmq")
    )
    return connection.channel()


@router.post("/tasks")
def create_task(task: TaskCreate):
    job_id = str(uuid.uuid4())

    create_job(job_id)

    channel = get_channel()

    message = {
        "job_id": job_id,
        "task": task.task
    }

    channel.basic_publish(
        exchange="",
        routing_key="task_queue",
        body=json.dumps(message)
    )

    return {
        "job_id": job_id,
        "status": JobStatus.QUEUED
    }