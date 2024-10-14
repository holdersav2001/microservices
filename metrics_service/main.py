import os
import logging
from fastapi import FastAPI
from logging.handlers import RotatingFileHandler

app = FastAPI()

class ConfigLoader:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.config = self._load_config()

    def _load_config(self):
        return {
            'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        }

    def get(self, key: str) -> str:
        return self.config.get(key, '')

class Metrics:
    def __init__(self, config: ConfigLoader, logger: logging.Logger):
        self.config = config
        self.logger = logger

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
    logger.info("Metrics service starting up")

@app.get("/metrics")
async def get_metrics():
    return {"message": "Metrics endpoint. Prometheus integration has been removed."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)