import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status
from app.schemas import RepurposeRequest, RepurposeAcceptedResponse
from app.kafka_client import kafka_manager
from app.config import settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_manager.start_producer()
    yield
    await kafka_manager.stop_producer()

app = FastAPI(title="Enterprise AI Orchestrator API", lifespan=lifespan)

@app.get("/healthz", status_code=status.HTTP_200_OK)
async def health_check():
    return {"status": "healthy"}

@app.post("/api/v1/repurpose", response_model=RepurposeAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
async def submit_content_task(payload: RepurposeRequest):
    try:
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        event_payload = {
            "job_id": job_id,
            "user_id": payload.user_id,
            "raw_content": payload.raw_content
        }
        await kafka_manager.publish_event(settings.KAFKA_TOPIC_INPUT, event_payload)
        return RepurposeAcceptedResponse(job_id=job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka Ingestion Error: {str(e)}")