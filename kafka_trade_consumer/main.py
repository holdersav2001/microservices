import os
import uuid
import json
import logging
import signal
from datetime import datetime
from configparser import ConfigParser
from typing import Dict, Any, List
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import ConsumerStoppedError, KafkaConnectionError
from logging.handlers import RotatingFileHandler
import time
import pyinstrument

class PerformanceStats:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.total_messages = 0
        self.total_time = 0
        self.max_time = float('-inf')
        self.min_time = float('inf')
        self.batch_start_time = None
        self.batch_messages = 0

    def update(self, messages: int, process_time: float):
        self.total_messages += messages
        self.total_time += process_time
        self.max_time = max(self.max_time, process_time)
        self.min_time = min(self.min_time, process_time)
        
        if self.batch_start_time is None:
            self.batch_start_time = time.time()
        
        self.batch_messages += messages
        
        if self.batch_messages >= 1000:
            batch_end_time = time.time()
            batch_elapsed_time = batch_end_time - self.batch_start_time
            self.logger.info(f"Batch Statistics: Processed {self.batch_messages} messages in {batch_elapsed_time:.2f} seconds")
            self.logger.info(f"Start time: {datetime.fromtimestamp(self.batch_start_time)}, End time: {datetime.fromtimestamp(batch_end_time)}")
            self.batch_start_time = None
            self.batch_messages = 0

    def log_stats(self):
        avg_time = self.total_time / self.total_messages if self.total_messages > 0 else 0
        self.logger.info("Kafka Trade Consumer Performance Statistics:")
        self.logger.info(f"Total messages processed: {self.total_messages}")
        self.logger.info(f"Total processing time: {self.total_time:.2f} seconds")
        self.logger.info(f"Average processing time per message: {avg_time:.6f} seconds")
        self.logger.info(f"Maximum batch processing time: {self.max_time:.6f} seconds")
        self.logger.info(f"Minimum batch processing time: {self.min_time:.6f} seconds")

class ConfigLoader:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, str]:
        config = {
            'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'KAFKA_TOPIC': os.getenv('KAFKA_TOPIC', 'trade_events'),
            'KAFKA_GROUP_ID': os.getenv('KAFKA_GROUP_ID', 'trade_consumer_group'),
            'KAFKA_DLQ_TOPIC': os.getenv('KAFKA_DLQ_TOPIC', 'trade_events_errors'),
            'KAFKA_PROCESSED_TOPIC': os.getenv('KAFKA_PROCESSED_TOPIC', 'trade_events_consumed'),
        }
        self.logger.info(f"Loaded configuration: {config}")
        return config

    def get(self, key: str) -> str:
        return self.config.get(key, '')

class KafkaTradeConsumer:
    def __init__(self, config: ConfigLoader, logger: logging.Logger, stats_logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.consumer: AIOKafkaConsumer = None
        self.producer: AIOKafkaProducer = None
        self.running = False
        self.batch_size = 100
        self.stats = PerformanceStats(stats_logger)
        self.profiler = pyinstrument.Profiler()

    async def setup_consumer(self) -> None:
        max_retries = 30
        base_delay = 5  # seconds
        max_delay = 60  # seconds

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})")
                self.logger.info(f"Using bootstrap servers: {self.config.get('KAFKA_BOOTSTRAP_SERVERS')}")
                self.consumer = AIOKafkaConsumer(
                    self.config.get('KAFKA_TOPIC'),
                    bootstrap_servers=self.config.get('KAFKA_BOOTSTRAP_SERVERS'),
                    group_id=self.config.get('KAFKA_GROUP_ID'),
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    max_poll_interval_ms=300000,  # 5 minutes
                    max_poll_records=self.batch_size
                )
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=self.config.get('KAFKA_BOOTSTRAP_SERVERS')
                )
                await self.consumer.start()
                await self.producer.start()
                self.logger.info(f'Successfully connected to Kafka and subscribed to {self.config.get("KAFKA_TOPIC")} topic')
                return
            except KafkaConnectionError as e:
                if attempt < max_retries - 1:
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    self.logger.warning(f"Failed to connect to Kafka (attempt {attempt + 1}/{max_retries}). Retrying in {delay} seconds... Error: {str(e)}")
                    await asyncio.sleep(delay)
                else:
                    self.logger.error(f"Failed to connect to Kafka after {max_retries} attempts: {str(e)}")
                    raise

    async def send_to_dlq(self, msg: Any, error_info: str) -> None:
        try:
            dlq_message = {
                'original_message': msg.value.decode('utf-8') if msg.value else None,
                'error_info': error_info,
                'topic': msg.topic,
                'partition': msg.partition,
                'offset': msg.offset,
                'timestamp': datetime.fromtimestamp(msg.timestamp / 1000).isoformat(),
                'headers': [(k, v.decode('utf-8') if v else None) for k, v in msg.headers] if msg.headers else None
            }
            
            dlq_message_json = json.dumps(dlq_message).encode('utf-8')
            
            await self.producer.send_and_wait(
                self.config.get('KAFKA_DLQ_TOPIC'),
                dlq_message_json,
                key=msg.key
            )
            self.logger.info(f"Message sent to DLQ: {msg.key}, Error: {error_info}")
        except Exception as e:
            self.logger.error(f"Failed to send message to DLQ: {str(e)}")

    async def process_message_batch(self, messages: List[Any]) -> int:
        start_time = time.time()
        self.logger.info(f"Processing batch of {len(messages)} messages")
        processed_count = 0
        for msg in messages:
            if await self.process_message(msg):
                processed_count += 1
        
        process_time = time.time() - start_time
        self.stats.update(processed_count, process_time)
        self.logger.info(f"Processed {processed_count} messages successfully, {len(messages) - processed_count} errors")
        return processed_count

    async def process_message(self, msg: Any) -> bool:
        try:
            trade_event = json.loads(msg.value.decode('utf-8'))
            # Add any necessary processing logic here
            
            # Send processed message to the trade_events_consumed topic
            await self.producer.send_and_wait(
                self.config.get('KAFKA_PROCESSED_TOPIC'),
                json.dumps(trade_event).encode('utf-8'),
                key=msg.key
            )
            return True
        except json.JSONDecodeError as e:
            await self.send_to_dlq(msg, f"JSON decode error: {str(e)}")
            return False
        except Exception as e:
            await self.send_to_dlq(msg, f"Unexpected error: {str(e)}")
            return False

    async def consume_messages(self) -> None:
        self.running = True

        try:
            self.profiler.start()
            while self.running:
                try:
                    messages = await self.consumer.getmany(timeout_ms=1000, max_records=self.batch_size)
                    
                    if not messages:
                        self.logger.info("No messages received. Continuing to poll...")
                        await asyncio.sleep(1)  # Add a small delay to reduce CPU usage
                        continue

                    processed_count = 0

                    for _, batch in messages.items():
                        processed_count += await self.process_message_batch(batch)

                    self.logger.info(f"Processed {processed_count} messages")
                    await self.consumer.commit()

                except ConsumerStoppedError:
                    self.logger.error("Kafka consumer has been stopped. Exiting consume loop.")
                    break
                except Exception as e:
                    self.logger.error(f"Unexpected error during message consumption: {str(e)}")
                    await asyncio.sleep(5)  # Add a delay before retrying

        finally:
            self.profiler.stop()
            profiling_data = self.profiler.output_text(unicode=True, color=True)
            with open('pyinstrument_profiling.log', 'w', encoding='utf-8') as f:
                f.write(profiling_data)
            self.logger.info(f"Pyinstrument profiling data written to pyinstrument_profiling.log")
            await self.cleanup()
            self.stats.log_stats()

    async def cleanup(self) -> None:
        self.logger.info("Cleaning up resources...")
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

    def stop(self) -> None:
        self.running = False

def setup_logger(name: str, log_file: str, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    file_handler = RotatingFileHandler(log_file, maxBytes=10000000, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

async def main() -> None:
    logger = setup_logger('kafka_consumer', 'kafka_consumer.log')
    stats_logger = setup_logger('kafka_consumer_stats', 'kafka_consumer_stats.log')
    config = ConfigLoader(logger)

    consumer = KafkaTradeConsumer(config, logger, stats_logger)
    
    try:
        await consumer.setup_consumer()
    except Exception as e:
        logger.error(f"Failed to set up consumer: {str(e)}")
        return

    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("Received termination signal. Stopping consumer...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda _signum, _frame: signal_handler())

    try:
        consumer_task = asyncio.create_task(consumer.consume_messages())
        stop_task = asyncio.create_task(stop_event.wait())

        done, pending = await asyncio.wait(
            [consumer_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED
        )

        if consumer_task in done:
            logger.info("Consumer has finished processing all messages.")
        else:
            logger.info("Stopping consumer due to received signal.")
            consumer.stop()
            await consumer_task

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    except Exception as e:
        logger.error(f"Unexpected error in main: {str(e)}")
    finally:
        await consumer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
