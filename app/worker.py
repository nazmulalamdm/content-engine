import asyncio
import json
import logging
from app.config import settings
from app.kafka_client import get_kafka_consumer, kafka_manager
from app.graph import orchestrator_app

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WorkerLogger")

async def run_worker():
    consumer = None

    # কাফকা ব্রোকার প্রস্তুত না হওয়া পর্যন্ত অপেক্ষা ও পুনঃসংযোগের লজিক
    while True:
        try:
            logger.info("Connecting to Kafka broker...")
            consumer = get_kafka_consumer()
            await consumer.start()
            await kafka_manager.start_producer()
            logger.info("Worker started listening to Kafka topic: %s", settings.KAFKA_TOPIC_INPUT)
            break
        except Exception as e:
            if consumer:
                await consumer.stop()
            logger.warning(f"Kafka not ready yet ({e}). Retrying connection in 5 seconds...")
            await asyncio.sleep(5)

    try:
        async for msg in consumer:
            task = msg.value
            job_id = task["job_id"]
            logger.info("Processing Task ID: %s", job_id)

            initial_state = {
                "job_id": job_id,
                "user_id": task["user_id"],
                "raw_content": task["raw_content"],
                "key_points": [],
                "linkedin_post": "",
                "twitter_thread": [],
                "newsletter": ""
            }

            # গ্রাফ রান করা
            final_result = await orchestrator_app.ainvoke(initial_state)

            # আউটপুট কাফকার কমপ্লিশন টপিকে পাঠিয়ে দেওয়া
            await kafka_manager.publish_event(settings.KAFKA_TOPIC_OUTPUT, final_result)
            logger.info("Successfully completed job: %s and pushed to %s", job_id, settings.KAFKA_TOPIC_OUTPUT)
    except Exception as e:
        logger.error(f"Unexpected error during task consumption: {e}")
    finally:
        if consumer:
            await consumer.stop()
        await kafka_manager.stop_producer()

if __name__ == "__main__":
    asyncio.run(run_worker())