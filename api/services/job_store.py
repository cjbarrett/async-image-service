from models.job_status import JobStatus

jobs = {}

def create_job(job_id: str, task: str):
    jobs[job_id] = {
        "status": JobStatus.QUEUED,
        "task": task
    }

def update_job(job_id: str, status: str):
    if job_id in jobs:
        jobs[job_id]["status"] = status

def get_job(job_id: str):
    return jobs.get(job_id)