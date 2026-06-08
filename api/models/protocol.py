from enum import Enum

class WSMessageType(Enum):
    CONNECTION = "connection"
    JOB_UPDATE = "job_update"

class WSMessageMode(Enum):
    REPLAY = "replay"
    LIVE = "live"

