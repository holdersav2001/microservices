"""
MongoDB Helper for Batch Operations

This module provides a helper class for batch operations with MongoDB,
specifically tailored for event processing and metadata management.

Updated: [Current Date]
Changes:
- Added type hints for improved code clarity and static type checking
- Implemented batch insert operations for events
- Added methods for querying and updating event metadata
- Improved error handling and logging
- Enhanced logging in bulk_insert_events method
- Updated to use configurable database and collection names
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError
from datetime import datetime
from bson.objectid import ObjectId
from typing import Dict, List, Any
import logging
from motor.motor_asyncio import AsyncIOMotorClient

class AsyncMongoDBHelperBatch:
    def __init__(self, config: Dict[str, str], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.client = None
        self.db = None
        self.event_collection = None
        self.metadata_collection = None


    async def close(self):
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed.")


    async def initialize(self):
        try:
            self.client = AsyncIOMotorClient(self.config['MONGO_URI'])
            db_name = self.config['MONGO_URI'].split('/')[-1]
            self.db = self.client[db_name]
            self.event_collection = self.db[self.config['MONGO_COLLECTION']]
            self.metadata_collection = self.db['event_metadata']
            self.logger.info("MongoDB connection established.")
        except Exception as e:
            self.logger.error(f"Error initializing MongoDB collections: {e}")
            raise


    def _serialize_id(self, doc: Any) -> Any:
        try:
            if isinstance(doc, list):
                for item in doc:
                    if '_id' in item:
                        item['_id'] = str(item['_id'])
            elif isinstance(doc, dict):
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            else:
                self.logger.error(f"Unexpected document structure: {doc}")
        except Exception as e:
            self.logger.error(f"Error in _serialize_id method: {str(e)}")
        return doc

    async def bulk_insert_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        try:
            self.logger.info(f"Attempting to bulk insert {len(events)} documents")
            
            # Log the first few events for debugging
            for i, event in enumerate(events[:5]):
                self.logger.debug(f"Sample event {i + 1}: {event}")
            
            # Count events and outcomes
            event_count = sum(1 for e in events if e.get('type') == 'event')
            outcome_count = sum(1 for e in events if e.get('type') == 'outcome')
            self.logger.info(f"Inserting {event_count} events and {outcome_count} outcomes")

            result = await self.event_collection.insert_many(events)
            self.logger.info(f"Inserted {len(result.inserted_ids)} documents in bulk")
            
            return {
                'success': True, 
                'message': f'Inserted {len(result.inserted_ids)} documents successfully',
                'events_inserted': event_count,
                'outcomes_inserted': outcome_count
            }
        except Exception as e:
            self.logger.error(f"Error bulk inserting events: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    async def get_event_by_name_status_date(self, event_name: str, event_status: str, business_date: str) -> Dict[str, Any]:
        try:
            cursor = self.event_collection.find({
                'eventName': event_name,
                'eventStatus': event_status,
                'businessDate': business_date
            })
            events = await cursor.to_list(length=None)
            return {'success': True, 'data': events}
        except Exception as e:
            self.logger.error(f"Error retrieving events: {str(e)}")
            return {'success': False, 'error': str(e)}

    async def query_events_by_date(self, business_date: str) -> Dict[str, Any]:
        try:
            cursor = self.event_collection.find({'businessDate': business_date})
            events = await cursor.to_list(length=None)
            return {'success': True, 'data': events}
        except Exception as e:
            self.logger.error(f"Error querying events by date: {e}")
            return {'success': False, 'error': str(e)}
    def insert_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            self.event_collection.insert_one(event_data)
            self.logger.info(f"Inserted event: {event_data['eventId']}")
            return {'success': True, 'message': 'Event inserted successfully'}
        except Exception as e:
            self.logger.error(f"Error storing the event: {e}")
            return {'success': False, 'error': str(e)}

    def get_event_metadata_by_name_and_status(self, event_name: str, event_status: str) -> Dict[str, Any]:
        try:
            metadata = self.metadata_collection.find_one({
                'event_name': event_name,
                'event_status': event_status
            })
            if metadata:
                return {'success': True, 'data': self._serialize_id(metadata)}
            else:
                return {'success': False, 'error': 'Metadata not found'}
        except Exception as e:
            self.logger.error(f"Error retrieving event metadata: {str(e)}")
            return {'success': False, 'error': str(e)}

    def save_event_metadata_slo_sla(self, event_metadata: Dict[str, Any]) -> Dict[str, Any]:
        try:
            result = self.metadata_collection.update_one(
                {'event_name': event_metadata['event_name'], 'event_status': event_metadata['event_status']},
                {'$set': event_metadata},
                upsert=True
            )
            if result.modified_count > 0 or result.upserted_id:
                return {'success': True, 'message': 'Event metadata updated successfully'}
            else:
                return {'success': False, 'error': 'No changes made to event metadata'}
        except Exception as e:
            self.logger.error(f"Error saving event metadata: {str(e)}")
            return {'success': False, 'error': str(e)}