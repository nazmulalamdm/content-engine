from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    GROQ_API_KEY: str
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_TOPIC_INPUT: str = "content.repurpose.raw"
    KAFKA_TOPIC_OUTPUT: str = "content.repurpose.completed"
    KAFKA_CONSUMER_GROUP: str = "content-orchestrator-group"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()