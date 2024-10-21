import os
import json
import logging
from datetime import datetime, timezone
from configparser import ConfigParser
from typing import Dict, Any, List
import fastjsonschema
import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from logging.handlers import RotatingFileHandler
from services.event_services_batch import EventServicesBatch
from helpers.mongodb_helper_batch import MongoDBHelperBatch

import time

class PerformanceStats:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.total_events = 0
        self.total_time = 0
        self.max_time = float('-inf')
        self.min_time = float('inf')
        self.batch_start_time = None
        self.batch_events = 0
        self.batch_mongo_time = 0

    def update(self, process_time: float, mongo_time: float):
        self.total_events += 1
        self.total_time += process_time
        self.max_time = max(self.max_time, process_time)
        self.min_time = min(self.min_time, process_time)
        
        if self.batch_start_time is None:
            self.batch_start_time = time.time()
        
        self.batch_events += 1
        self.batch_mongo_time += mongo_time
        
        if self.batch_events >= 1000:
            batch_end_time = time.time()
            batch_elapsed_time = batch_end_time - self.batch_start_time
            self.logger.info(f"Batch Statistics: Processed {self.batch_events} events in {batch_elapsed_time:.2f} seconds")
            self.logger.info(f"Start time: {datetime.fromtimestamp(self.batch_start_time)}, End time: {datetime.fromtimestamp(batch_end_time)}")
            self.logger.info(f"Total MongoDB interaction time: {self.batch_mongo_time:.2f} seconds")
            self.batch_start_time = None
            self.batch_events = 0
            self.batch_mongo_time = 0

    def log_stats(self):
        avg_time = self.total_time / self.total_events if self.total_events > 0 else 0
        self.logger.info("Event Processing Service Performance Statistics:")
        self.logger.info(f"Total events processed: {self.total_events}")
        self.logger.info(f"Total processing time: {self.total_time:.2f} seconds")
        self.logger.info(f"Average processing time per event: {avg_time:.6f} seconds")
        self.logger.info(f"Maximum event processing time: {self.max_time:.6f} seconds")
        self.logger.info(f"Minimum event processing time: {self.min_time:.6f} seconds")

class ConfigLoader:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.config = self._load_config()
        self._log_config()

    def _load_config(self) -> Dict[str, str]:
        config = {
            'MONGO_URI': os.getenv('MONGO_URI', 'mongodb://192.168.0.120:27017/realtime'),
            'MONGO_COLLECTION': os.getenv('MONGO_COLLECTION', 'trade_events'),
            'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', '192.168.0.120:9092'),
            'KAFKA_TOPIC': os.getenv('KAFKA_TOPIC', 'trade_events_consumed'),
            'KAFKA_GROUP_ID': os.getenv('KAFKA_GROUP_ID', 'event_processing_group'),
        }
        return config

    def _log_config(self):
        self.logger.info("Configuration:")
        for key, value in self.config.items():
            self.logger.info(f"{key}: {value}")

    def get(self, key: str) -> str:
        return self.config.get(key, '')

class EventProcessor:
    def __init__(self, event_services: EventServicesBatch, logger: logging.Logger, stats_logger: logging.Logger):
        self.event_services = event_services
        self.logger = logger
        self.validate = self._create_validator()
        self.stats = PerformanceStats(stats_logger)

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

    def process_message(self, trade_event: Dict[str, Any]) -> Dict[str, Any]:
        start_time = time.time()
        if not self.validate_message(trade_event):
            raise ValueError("Invalid message format")

        transformed_event = self.transform_message(trade_event)
        
        mongo_start_time = time.time()
        result = self.event_services.insert_event(transformed_event)
        mongo_time = time.time() - mongo_start_time
        
        process_time = time.time() - start_time
        self.stats.update(process_time, mongo_time)
        
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
                self.logger.info(f"KAFKA_TOPIC: {self.config.get('KAFKA_TOPIC')}")
                self.logger.info(f"KAFKA_GROUP_ID: {self.config.get('KAFKA_GROUP_ID')}")
                self.logger.info(f"Environment variable KAFKA_BOOTSTRAP_SERVERS: {os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")
                self.consumer = AIOKafkaConsumer(
                    self.config.get('KAFKA_TOPIC'),
                    bootstrap_servers=bootstrap_servers_list,
                    group_id=self.config.get('KAFKA_GROUP_ID'),
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    session_timeout_ms=45000,
                    heartbeat_interval_ms=15000,
                    max_poll_interval_ms=300000,
                    max_poll_records=500
                )
                await self.consumer.start()
                self.logger.info(f"Kafka consumer started. Listening to topic: {self.config.get('KAFKA_TOPIC')}")
                return
            except KafkaConnectionError as e:
                self.logger.error(f"Failed to connect to Kafka (attempt {attempt + 1}/{max_retries}). Error: {str(e)}")
                self.logger.error(f"Error details: {e.__class__.__name__}: {str(e)}")
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
                    self.event_processor.process_message(trade_event)
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to decode message: {e}")
                except ValueError as e:
                    self.logger.error(f"Failed to process message: {e}")
                except Exception as e:
                    self.logger.error(f"Unexpected error processing message: {e}")
        finally:
            await self.consumer.stop()
            self.event_processor.stats.log_stats()

    def stop(self):
        self.running = False

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
    stats_logger = setup_logger('event_processing_stats', 'event_processing_stats.log')
    config = ConfigLoader(logger)
    mongodb_helper = MongoDBHelperBatch(config={
        'MONGO_URI': config.get('MONGO_URI'),
        'MONGO_COLLECTION': config.get('MONGO_COLLECTION')
    }, logger=logger)
    event_services = EventServicesBatch(db_helper=mongodb_helper, logger=logger)
    event_processor = EventProcessor(event_services, logger, stats_logger)
    kafka_consumer = KafkaEventConsumer(config, event_processor, logger)

    try:
        await kafka_consumer.setup_consumer()
        await kafka_consumer.consume_messages()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    except KeyboardInterrupt:
        logger.info("Received interrupt, shutting down...")
    finally:
        kafka_consumer.stop()
        event_services.flush_buffer()

if __name__ == "__main__":
    asyncio.run(main())
