import os
import logging
from fastapi import FastAPI
from prometheus_client import Counter, Histogram, CollectorRegistry, push_to_gateway
from logging.handlers import RotatingFileHandler
import aiohttp

app = FastAPI()

# Prometheus metrics setup
REGISTRY = CollectorRegistry()
MESSAGE_COUNTER = Counter('kafka_messages_processed', 'Number of Kafka messages processed', registry=REGISTRY)
MESSAGE_LATENCY = Histogram('kafka_message_latency_seconds', 'Message processing latency in seconds', registry=REGISTRY)
DLQ_COUNTER = Counter('dlq_messages', 'Number of messages sent to dead-letter queue', registry=REGISTRY)

class ConfigLoader:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.config = self._load_config()

    def _load_config(self):
        return {
            'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            'PROMETHEUS_PUSHGATEWAY': os.getenv('PROMETHEUS_PUSHGATEWAY', 'pushgateway:9091'),
        }

    def get(self, key: str) -> str:
        return self.config.get(key, '')

class Metrics:
    def __init__(self, config: ConfigLoader, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.prometheus_available = False

    async def check_prometheus_connection(self) -> None:
        pushgateway_url = f"http://{self.config.get('PROMETHEUS_PUSHGATEWAY')}"
        max_retries = 3
        retry_delay = 2  # seconds

        async with aiohttp.ClientSession() as session:
            for attempt in range(max_retries):
                try:
                    async with session.get(pushgateway_url, timeout=5) as response:
                        if response.status == 200:
                            self.prometheus_available = True
                            self.logger.info("Successfully connected to Prometheus Pushgateway")
                            return
                except aiohttp.ClientError:
                    self.logger.warning(f"Failed to connect to Prometheus Pushgateway (attempt {attempt + 1}/{max_retries})")
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)

        self.logger.warning("Unable to connect to Prometheus Pushgateway. Continuing without Prometheus metrics.")

    def record_metrics(self, num_messages: int, processing_time: float) -> None:
        if self.prometheus_available:
            MESSAGE_COUNTER.inc(num_messages)
            MESSAGE_LATENCY.observe(processing_time)
            try:
                push_to_gateway(self.config.get('PROMETHEUS_PUSHGATEWAY'), job='kafka_consumer', registry=REGISTRY)
            except Exception as e:
                self.logger.error(f"Failed to push metrics to Prometheus: {e}")

    def record_dlq_message(self) -> None:
        if self.prometheus_available:
            DLQ_COUNTER.inc()
            try:
                push_to_gateway(self.config.get('PROMETHEUS_PUSHGATEWAY'), job='kafka_consumer', registry=REGISTRY)
            except Exception as e:
                self.logger.error(f"Failed to push DLQ metric to Prometheus: {e}")

def setup_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    file_handler = RotatingFileHandler('metrics_service.log', maxBytes=10000000, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

logger = setup_logger()
config = ConfigLoader(logger)
metrics = Metrics(config, logger)

@app.on_event("startup")
async def startup_event():
    await metrics.check_prometheus_connection()

@app.get("/metrics")
async def get_metrics():
    return {
        "messages_processed": MESSAGE_COUNTER._value.get(),
        "average_latency": MESSAGE_LATENCY._sum.get() / MESSAGE_COUNTER._value.get() if MESSAGE_COUNTER._value.get() > 0 else 0,
        "dlq_messages": DLQ_COUNTER._value.get()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)