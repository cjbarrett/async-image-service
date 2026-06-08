import os

# TODO - Get rid of this redundant code - it exists here and in the worker - fix it!
# ------------------------------------------------------------------
# 1. INFRASTRUCTURE CONFIGURATIONS (Environment-Dependent)
# ------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))


# ------------------------------------------------------------------
# 2. APPLICATION CONSTANTS (Domain-Specific, with Env Overrides)
# ------------------------------------------------------------------
# We centralize these here so your entire app shares the same stream definitions.
# We still use os.getenv as a wrapper just in case you want to scale horizontal consumers later!
STREAM_KEY = os.getenv("STREAM_KEY", "job_events_stream")
GROUP = os.getenv("GROUP", "ws_group")

# For the consumer name, using an env var lets you scale replicas 
# (e.g. ws_consumer_1, ws_consumer_2) in Kubernetes later!
CONSUMER = os.getenv("CONSUMER", "ws_1")
