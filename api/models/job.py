from pydantic import BaseModel
from models.job_status import JobStatus

class TaskCreate(BaseModel):
    task: str

class StatusUpdate(BaseModel):
    status: JobStatus


    