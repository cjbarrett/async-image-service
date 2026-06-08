import time
import pika
import json
import redis
from datetime import datetime

from deps import redis_client
from models.job_status import JobStatus
from config import REDIS_HOST, REDIS_PORT, RABBITMQ_HOST, RABBITMQ_PORT, STREAM_KEY
 
def update_job(job_id: str, **updates):
    data = redis_client.get(job_id)
    if not data:
        return

    job = json.loads(data)

    job.update(updates)
    job["updated_at"] = datetime.now().isoformat()

    redis_client.set(job_id, json.dumps(job))


def publish_event(job_id: str, status: str):
    event = {
        "job_id": str(job_id),
        "status": str(status)
    }

    redis_client.xadd(STREAM_KEY, event)


def callback(ch, method, properties, body):
    message = json.loads(body)

    job_id = message["job_id"]
    task = message.get("task")

    print(f"[WORKER] Received job_id={job_id}, task={task}")

    update_job(job_id, status=JobStatus.PROCESSING)
    publish_event(job_id, JobStatus.PROCESSING)

    try:
        time.sleep(5)

        update_job(job_id, status=JobStatus.COMPLETED)
        publish_event(job_id, JobStatus.COMPLETED)

        print(f"[WORKER] Completed job_id={job_id}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        update_job(job_id, status="failed")
        publish_event(job_id, "failed")

        print(f"[WORKER] Failed job_id={job_id}: {e}")

        ch.basic_ack(delivery_tag=method.delivery_tag)


def connect_rabbitmq():
    for i in range(20):
        try:
            print(f"[WORKER] connecting attempt {i+1}", flush=True)

            return pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                    connection_attempts=10,
                    retry_delay=2,
                )
            )
        except Exception as e:
            print(f"[WORKER] retrying: {e}")
            time.sleep(2)

    raise Exception("RabbitMQ never became available")


def main():
    connection = connect_rabbitmq()
    channel = connection.channel()

    channel.queue_declare(queue="task_queue", durable=True)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue="task_queue",
        on_message_callback=callback,
        auto_ack=False
    )

    print("[WORKER] Waiting for messages...", flush=True)

    channel.start_consuming()


if __name__ == "__main__":
    main()