# backend/app/routes.py
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from uuid import uuid4
from .cache import make_cache_key_for_code, get_result
from .tasks import explain_code_task
from .schemas import CodeRequest, ExplanationResponse, JobResponse
import logging

router = APIRouter()

@router.post("/explain/pyspark", response_model=JobResponse)
async def explain_pyspark(request: CodeRequest):
    code = request.code
    cache_key = make_cache_key_for_code(code)
    # 1) Check cache
    cached = get_result(cache_key)
    if cached:
        # cached payload includes job_id & result
        return {"job_id": cached.get("job_id", "cached"), "status": cached["status"], "cached": True}

    # 2) Enqueue background job
    job_id = str(uuid4())
    # start Celery task (non-blocking) - pass job_id and cache_key
    explain_code_task.apply_async(kwargs={"job_id": job_id, "code": code, "cache_key": cache_key})
    # we return job_id to client which will poll /status/<job_id>
    return {"job_id": job_id, "status": "pending", "cached": False}
"""
    result = llm.explain_pyspark(request.code)
    logging.info(f"\n\nTokens used: {result['tokens_used']}, \nPrompt tokens: {result['prompt_tokens']}, \nCompletion tokens: {result['completion_tokens']}, \nLatency: {result['latency_ms']} ms\n")

    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return ExplanationResponse(
        explanation=result.get("explanation"),
        latency_ms=result["latency_ms"],
        tokens_used=result.get("tokens_used"),
        propmt_tokens=result.get("prompt_tokens"),
        completion_tokens=result.get("completion_tokens"),
        error=None
    )
"""
@router.get("/status/{job_id}", response_model=ExplanationResponse)
async def get_status(job_id: str):
    job_key = f"job:{job_id}"
    payload = get_result(job_key)
    if not payload:
        return {"job_id": job_id, "status": "pending", "result": None}
    return payload

@router.get("/health")
async def health():
    return {"status": "ok"}
