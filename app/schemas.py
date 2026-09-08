from typing import List, Optional
from pydantic import BaseModel, Field

class RepurposeRequest(BaseModel):
    user_id: str = Field(..., example="usr_101")
    raw_content: str = Field(..., min_length=50, example="Paste full article here...")

class RepurposeAcceptedResponse(BaseModel):
    job_id: str
    status: str = "QUEUED"
    message: str = "Request queued into Kafka. Worker is processing asynchronously."

class RepurposedOutput(BaseModel):
    job_id: str
    user_id: str
    key_points: List[str]
    linkedin_post: str
    twitter_thread: List[str]
    newsletter: str