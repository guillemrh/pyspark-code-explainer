# backend/app/tasks.py
import time
import os
from celery import Celery
from celery.utils.log import get_task_logger
from .config import settings
from .llm import GeminiClient
from .cache import set_result

# ----------------- CELERY CONFIGURATION (The Delivery System) -----------------
logger = get_task_logger(__name__)

# The Celery app is initialized.
# It uses settings.redis_url for both the broker (the queue) and the backend (where results are stored).
celery = Celery(
    "tasks",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

# ----------------- TASK DEFINITION AND RETRY POLICY (The Chef's Instructions) -----------------
# @celery.task decorator marks this function as a worker job.
@celery.task(
    bind=True,                 # Gives access to 'self' (the task instance) for retries/context.
    autoretry_for=(Exception,),# Automatically retry if *any* Exception occurs during execution.
    retry_backoff=True,        # Exponentially increase the delay between retries (e.g., 1s, 2s, 4s).
    retry_kwargs={'max_retries': 3} # Limit retries to 3 times (4 attempts total: 1st run + 3 retries).
)
def explain_code_task(self, job_id: str, code: str, cache_key: str):
    """
    Celery task to call Gemini and store result in Redis.
    """
    logger.info(f"Task start job_id={job_id}")
    
    # ----------------- CORE LOGIC (The Slow Work) -----------------
    client = GeminiClient()
    start = time.time()
    
    # This is the slow operation that we moved to a background worker
    result = client.explain_pyspark(code) 
    
    duration = int((time.time() - start) * 1000)
    
    # ----------------- RESULT PROCESSING -----------------
    payload = {
        "job_id": job_id,
        "status": "finished" if "error" not in result else "failed",
        "result": result,
        "duration_ms": duration
    }
    
    # ----------------- CACHE STORAGE -----------------
    # store result for cache hits (i.e., someone requests the same code explanation again)
    set_result(cache_key, payload, ttl=settings.cache_ttl) 
    
    # store result for *this specific job* (i.e., status check for a running job)
    set_result(f"job:{job_id}", payload, ttl=settings.cache_ttl) 
    
    logger.info(f"Task finished job_id={job_id} status={payload['status']}")
    return payload