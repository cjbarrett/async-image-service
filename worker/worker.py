import pika
import json
import time

QUEUE_NAME = "task_queue"


def connect_with_retry():
    print("[WORKER] Starting connection to RabbitMQ...", flush=True)

    for attempt in range(10):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
            print("[WORKER] Connected to RabbitMQ", flush=True)
            return connection
        except Exception as e:
            print(
                f"[WORKER] RabbitMQ not ready (attempt {attempt + 1}/10): {e}",
                flush=True,
            )
            time.sleep(2)

    raise Exception("Could not connect to RabbitMQ after retries")


def callback(ch, method, properties, body):
    try:
        task = json.loads(body)
        print(f"[WORKER] Received task: {task}", flush=True)

        # simulate work
        time.sleep(2)

        print(f"[WORKER] Finished task: {task}", flush=True)

    except Exception as e:
        print(f"[WORKER] Error processing message: {e}", flush=True)


def main():
    print("[WORKER] Booting worker...", flush=True)

    connection = connect_with_retry()
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME)

    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback,
        auto_ack=True,
    )

    print("[WORKER] Waiting for messages...", flush=True)

    channel.start_consuming()


if __name__ == "__main__":
    main()