import time
import pika
import json


def callback(ch, method, properties, body):
    print(f"[WORKER] Received: {body.decode()}")


def connect_rabbitmq():
    for i in range(20):
        try:
            print(f"[WORKER] connecting attempt {i+1}", flush=True)

            return pika.BlockingConnection(
                pika.ConnectionParameters(
                    host="rabbitmq",
                    port=5672,
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
        auto_ack=True
    )

    print("[WORKER] Waiting for messages...", flush=True)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("[WORKER] shutting down...")
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == "__main__":
    main()



