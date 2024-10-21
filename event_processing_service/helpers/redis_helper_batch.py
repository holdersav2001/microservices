import redis.asyncio as redis
from typing import Dict, Any, List
import logging
import json
from bson import ObjectId
from datetime import datetime

class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class AsyncRedisHelperBatch:
    def __init__(self, config: Dict[str, str], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.redis_client = None

    async def initialize(self):
        try:
            self.redis_client = redis.Redis(
                host=self.config['REDIS_HOST'],
                port=int(self.config['REDIS_PORT']),
                db=int(self.config['REDIS_DB']),
                decode_responses=True
            )
            await self.redis_client.ping()
            self.logger.info("Redis connection established.")
        except Exception as e:
            self.logger.error(f"Redis connection failed: {e}")
            raise

    async def set_event(self, key: str, event_data: str) -> bool:
        try:
            await self.redis_client.set(key, event_data)
            return True
        except Exception as e:
            self.logger.error(f"Error setting event in Redis: {e}")
            return False

    async def bulk_set_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        try:
            pipeline = self.redis_client.pipeline()
            for event in events:
                key = f"{event['eventName']}:{event['businessDate']}:{event['eventStatus']}:{event['type']}"
                # Use the custom encoder here
                event_json = json.dumps(event, cls=MongoJSONEncoder)
                pipeline.set(key, event_json)
            await pipeline.execute()
            self.logger.info(f"Bulk set {len(events)} events in Redis")
            return {'success': True, 'message': f'Bulk set {len(events)} events in Redis'}
        except Exception as e:
            self.logger.error(f"Error bulk setting events in Redis: {e}")
            return {'success': False, 'error': str(e)}

    async def get_event(self, key: str) -> str:
        try:
            return await self.redis_client.get(key)
        except Exception as e:
            self.logger.error(f"Error getting event from Redis: {e}")
            return None

    async def delete_event(self, key: str) -> bool:
        try:
            await self.redis_client.delete(key)
            return True
        except Exception as e:
            self.logger.error(f"Error deleting event from Redis: {e}")
            return False

    async def key_exists(self, key: str) -> bool:
        try:
            exists = await self.redis_client.exists(key)
            return exists == 1
        except Exception as e:
            self.logger.error(f"Error checking if key exists in Redis: {e}")
            return False

    async def close(self):
        if self.redis_client:
            await self.redis_client.close()
            self.logger.info("Redis connection closed.")
