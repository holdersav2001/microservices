import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any
import asyncio
import time
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from logging.handlers import RotatingFileHandler
from services.event_services_batch import EventServicesBatch
from helpers.mongodb_helper_batch import AsyncMongoDBHelperBatch
from helpers.redis_helper_batch import AsyncRedisHelperBatch
import fastjsonschema
import pyinstrument

class ConfigLoader:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.config = self._load_config()
        self._log_config()

    def _load_config(self) -> Dict[str, str]:
        config = {
            'MONGO_URI': os.getenv('MONGO_URI', 'mongodb://localhost:27017/eventdb'),
            'MONGO_COLLECTION': os.getenv('MONGO_COLLECTION', 'trade_events'),
            'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'KAFKA_TOPIC': os.getenv('KAFKA_TOPIC', 'trade_events_consumed'),
            'KAFKA_GROUP_ID': os.getenv('KAFKA_GROUP_ID', 'event_processing_group'),
            'REDIS_HOST': os.getenv('REDIS_HOST', 'localhost'),
            'REDIS_PORT': os.getenv('REDIS_PORT', '6379'),
            'REDIS_DB': os.getenv('REDIS_DB', '0'),
        }
        return config

    def _log_config(self):
        self.logger.info("Configuration:")
        for key, value in self.config.items():
            self.logger.info(f"{key}: {value}")

    def get(self, key: str) -> str:
        return self.config.get(key, '')

class EventProcessor:
    def __init__(self, event_services: EventServicesBatch, logger: logging.Logger):
        self.event_services = event_services
        self.logger = logger
        self.validate = self._create_validator()

    def _create_validator(self):
        schema = {
            "type": "object",
            "properties": {
                "process_id": {"type": "string"},
                "trade_id": {"type": "integer"},
                "status": {"type": "string"},
                "timestamp": {"type": "string"},
                "application": {"type": "string"}
            },
            "required": ["trade_id", "status", "timestamp"]
        }
        return fastjsonschema.compile(schema)

    def validate_message(self, message: Dict[str, Any]) -> bool:
        try:
            self.validate(message)
            return True
        except fastjsonschema.JsonSchemaException:
            return False

    def transform_message(self, trade_event: Dict[str, Any]) -> Dict[str, Any]:
        trade_id = trade_event.get('trade_id', 'unknown')
        event_name = f"{trade_event.get('process_id', 'unknown')}-{trade_id}"
        
        event_time = datetime.fromisoformat(trade_event.get('timestamp')).replace(tzinfo=timezone.utc)
        
        return {
            'eventName': event_name,
            'tradeId': trade_id,
            'eventStatus': trade_event.get('status', 'unknown'),
            'businessDate': event_time.strftime('%Y-%m-%d'),
            'eventTime': event_time.isoformat(),
            'eventType': 'MESSAGE',
            'batchOrRealtime': 'Realtime',
            'resource': 'trading app',
            'message': '',
            'details': {
                'messageId': trade_id,
                'messageQueue': 'trade_events',
                'application': trade_event.get('application', 'unknown')
            }
        }

    async def process_message(self, trade_event: Dict[str, Any]) -> Dict[str, Any]:
        if not self.validate_message(trade_event):
            raise ValueError("Invalid message format")

        transformed_event = self.transform_message(trade_event)
        
        result = await self.event_services.insert_event(transformed_event)
        
        if result['success']:
            self.logger.debug(f"Processed and buffered event: {transformed_event}")
            return {"status": "success", "message": "Event processed successfully"}
        else:
            error_info = f"Failed to insert event: {result.get('error', 'Unknown error')}"
            self.logger.error(error_info)
            raise ValueError(error_info)

class KafkaEventConsumer:
    def __init__(self, config: ConfigLoader, event_processor: EventProcessor, logger: logging.Logger):
        self.config = config
        self.event_processor = event_processor
        self.logger = logger
        self.consumer = None
        self.running = False

    async def setup_consumer(self):
        max_retries = 30
        retry_delay = 10  # seconds

        for attempt in range(max_retries):
            try:
                bootstrap_servers = self.config.get('KAFKA_BOOTSTRAP_SERVERS')
                bootstrap_servers_list = [server.strip() for server in bootstrap_servers.split(',')]
                self.logger.info(f"Attempting to connect to Kafka at {bootstrap_servers} (attempt {attempt + 1}/{max_retries})")
                self.consumer = AIOKafkaConsumer(
                    self.config.get('KAFKA_TOPIC'),
                    bootstrap_servers=bootstrap_servers_list,
                    group_id=self.config.get('KAFKA_GROUP_ID'),
                    auto_offset_reset='earliest',
                    enable_auto_commit=False
                )
                await self.consumer.start()
                self.logger.info(f"Kafka consumer started. Listening to topic: {self.config.get('KAFKA_TOPIC')}")
                return
            except KafkaConnectionError as e:
                self.logger.error(f"Failed to connect to Kafka (attempt {attempt + 1}/{max_retries}). Error: {str(e)}")
                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    self.logger.error(f"Failed to connect to Kafka after {max_retries} attempts. Giving up.")
                    raise

    async def consume_messages(self):
        self.running = True
        try:
            async for msg in self.consumer:
                if not self.running:
                    break
                try:
                    trade_event = json.loads(msg.value.decode('utf-8'))
                    await self.event_processor.process_message(trade_event)
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to decode message: {e}")
                except ValueError as e:
                    self.logger.error(f"Failed to process message: {e}")
                except Exception as e:
                    self.logger.error(f"Unexpected error processing message: {e}")
        finally:
            await self.consumer.stop()

    def stop(self):
        self.running = False

async def periodic_flush(event_services: EventServicesBatch):
    while True:
        await asyncio.sleep(5)  # Check every minute
        if time.time() - event_services.last_flush_time > event_services.flush_interval:
            await event_services.flush_buffer()

def setup_logger(name: str, log_file: str, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    file_handler = RotatingFileHandler(log_file, maxBytes=10000000, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

async def main():
    logger = setup_logger('event_processing', 'event_processing.log')
    config = ConfigLoader(logger)
    
    # Capture profiling report
    profiler = pyinstrument.Profiler()
    profiler.start()
    
    mongodb_helper = AsyncMongoDBHelperBatch(config={
        'MONGO_URI': config.get('MONGO_URI'),
        'MONGO_COLLECTION': config.get('MONGO_COLLECTION')
    }, logger=logger)
    await mongodb_helper.initialize()
    
    redis_helper = AsyncRedisHelperBatch(config={
        'REDIS_HOST': config.get('REDIS_HOST'),
        'REDIS_PORT': config.get('REDIS_PORT'),
        'REDIS_DB': config.get('REDIS_DB')
    }, logger=logger)
    await redis_helper.initialize()
    
    event_services = EventServicesBatch(mongodb_helper, redis_helper, logger)
    event_processor = EventProcessor(event_services, logger)
    kafka_consumer = KafkaEventConsumer(config, event_processor, logger)

    # Start periodic flush task
    flush_task = asyncio.create_task(periodic_flush(event_services))
    
    try:
        # Start time
        start_time = time.time()

        await kafka_consumer.setup_consumer()
        await kafka_consumer.consume_messages()

        # End time
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time:.2f} seconds")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    except KeyboardInterrupt:
        
        logger.info("Received keyboard interrupt. Stopping consumer...")

        kafka_consumer.stop()
        await event_services.manual_flush()
        await mongodb_helper.close()
        await redis_helper.close()
        


    finally:
        kafka_consumer.stop()
        await event_services.manual_flush()  # Ensure any remaining events are flushed
        await mongodb_helper.close()
        await redis_helper.close()
        flush_task.cancel()
        try:
            await flush_task
        except asyncio.CancelledError:
            pass
        
        profiler.stop()
        print(profiler.output_text(unicode=True, color=True))

if __name__ == "__main__":
    asyncio.run(main())
