# backend/app/tasks.py
import time
from celery import Celery
from celery.utils.log import get_task_logger
from .config import settings, CACHE_TTL
from .llm import GeminiClient
from .cache import set_result

logger = get_task_logger(__name__)

celery = Celery(
    "tasks",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

@celery.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
)
def explain_code_task(self, job_id: str, code: str, cache_key: str):
    """
    Background task that:
    1. Executes the LLM call
    2. Stores reusable result in cache
    3. Stores execution-specific job status
    """

    logger.info(f"Task start job_id={job_id}")

    # ⏱ Measure FULL job duration (queue + execution)
    task_start = time.time()

    # simulate queue / scheduling delay
    time.sleep(3)

    client = GeminiClient()
    result = client.explain_pyspark(code)

    job_duration_ms = int((time.time() - task_start) * 1000)

    status = "failed" if "error" in result else "finished"

    # 1️⃣ Cache key (reusable content)
    set_result(cache_key, result, ttl=CACHE_TTL)
    
    # 2️⃣ Job key (execution metadata + content)
    job_payload = {
        "job_id": job_id,
        "status": status,
        "result": result,  # ✅ Direct ExplanationResult
        "job_duration_ms": job_duration_ms,
        "cached": False
    }
    
    set_result(f"job:{job_id}", job_payload, ttl=CACHE_TTL)
    
    logger.info(
        f"Task finished job_id={job_id} "
        f"status={status} "
        f"job_duration_ms={job_duration_ms}"
    )

    return job_payload
