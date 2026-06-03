from fastapi import FastAPI
from pydantic import BaseModel
import pika
import json

app = FastAPI()

class Task(BaseModel):
    task_name: str

@app.post("/task")
def create_task(task: Task):

    message = {"task": task.task_name}

    connection = pika.BlockingConnection(
        pika.ConnectionParameters("rabbitmq")
    )

    channel = connection.channel()

    channel.queue_declare(queue="task_queue")

    message = {
        "task": task.task_name
    }

    channel.basic_publish(
        exchange="",
        routing_key="task_queue",
        body=json.dumps(message)
    )

    connection.close()

    return {
        "message": "Task submitted",
        "task": task.task_name
    }