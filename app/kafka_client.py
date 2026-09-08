import json
import logging
import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app.config import settings

logger = logging.getLogger(__name__)

class KafkaManager:
    def __init__(self):
        self.producer: AIOKafkaProducer = None

    async def start_producer(self):
        # কাফকা ব্রোকার পুরোপুরি প্রস্তুত হতে ৮০ সেকেন্ড পর্যন্ত অপেক্ষা করবে
        for attempt in range(20):
            try:
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                await self.producer.start()
                logger.info("Kafka Producer successfully started.")
                return
            except Exception as e:
                # আনক্লোজড অবজেক্ট ওয়ার্নিং এড়াতে ক্লিনআপ
                if self.producer:
                    try:
                        await self.producer.stop()
                    except Exception:
                        pass
                    self.producer = None
                
                logger.warning(f"Waiting for Kafka broker... (attempt {attempt + 1}/20): {e}")
                await asyncio.sleep(4)

        raise RuntimeError("Could not connect to Kafka after multiple attempts.")

    async def stop_producer(self):
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka Producer stopped.")

    async def publish_event(self, topic: str, data: dict):
        if not self.producer:
            raise RuntimeError("Kafka Producer is not running.")
        await self.producer.send_and_wait(topic, data)

kafka_manager = KafkaManager()

def get_kafka_consumer() -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        settings.KAFKA_TOPIC_INPUT,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )