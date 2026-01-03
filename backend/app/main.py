# backend/app/main.py
import logging
from fastapi import FastAPI
from .api.routes import router
from app.logging import setup_logging

setup_logging(service_name="backend")

app = FastAPI(title="PySpark Code Reviewer", version="1.0.0")
app.include_router(router)

@app.get("/")
def root():
    return {"message": "PySpark LLM Explainer API running"}
