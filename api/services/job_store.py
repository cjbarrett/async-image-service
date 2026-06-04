from models.job_status import JobStatus

jobs = {}

def create_job(job_id: str):
    jobs[job_id] =JobStatus.QUEUED

def set_status(job_id: str, status: JobStatus):
    jobs[job_id] = status

def get_status(job_id: str):
    return jobs.get(job_id, None)