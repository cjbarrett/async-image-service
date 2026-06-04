from fastapi import APIRouter
import uuid
import json
import pika

from models.task import Task

router = APIRouter()

@router.post("/tasks")
def create_task(task: Task):
    job_id = str(uuid.uuid4())

    message = {
        "job_id": job_id,
        "task": task.task
    }

    connection = pika.BlockingConnection(
    pika.ConnectionParameters("rabbitmq")
)

    channel = connection.channel()
    channel.queue_declare(queue="task_queue")

    channel.basic_publish(
        exchange="",
        routing_key="task_queue",
        body=json.dumps(message)
    )

    return {
        "message": "Task submitted",
        "job_id": job_id
    }