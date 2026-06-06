from fastapi import FastAPI

from routes.tasks import router as tasks_router
from routes.ws import router as ws_router

app = FastAPI()

app.include_router(tasks_router)
app.include_router(ws_router)