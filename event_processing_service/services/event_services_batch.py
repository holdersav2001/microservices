import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Tuple
from utils.common import DateTimeUtils
import logging
import time
import asyncio

class EventServicesBatch:
    def __init__(self, db_helper: Any, redis_helper: Any, logger: logging.Logger):
        self.db_helper = db_helper
        self.redis_helper = redis_helper
        self.logger = logger
        self.event_buffer: List[Dict[str, Any]] = []
        self.outcome_buffer: List[Dict[str, Any]] = []
        self.buffer_size = 100  # Adjust this value based on your requirements
        self.last_flush_time = time.time()
        self.flush_interval = 5  # Flush every 60 seconds if not full

    async def insert_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert a single event and generate its outcome. This method buffers events and performs bulk insert when the buffer is full.
        """
        self.logger.info(f"Inserting event: {event_data}")
        transformed_event = await self.transform_event(event_data)
        if transformed_event:
            event_key = f"{transformed_event['eventName']}:{transformed_event['businessDate']}:{transformed_event['eventStatus']}:{transformed_event['type']}"
            key_exists = await self.redis_helper.key_exists(event_key)
            if not key_exists:
                self.event_buffer.append(transformed_event)
                self.logger.info(f"Event added to buffer. Buffer size: {len(self.event_buffer)}")
                
                if transformed_event['eventStatus'] != 'ERROR':
                    # Generate and buffer the outcome
                    existing_event_data = await self.db_helper.get_event_by_name_status_date(
                        transformed_event['eventName'],
                        transformed_event['eventStatus'],
                        transformed_event['businessDate']
                    )
                    existing_event_data = existing_event_data.get('data', [])
                    outcome = await self.insert_event_outcome(transformed_event, existing_event_data)
                    if outcome:
                        outcome_key = f"{outcome['eventName']}:{outcome['businessDate']}:{outcome['eventStatus']}:{outcome['type']}"
                        outcome_exists = await self.redis_helper.key_exists(outcome_key)
                        if not outcome_exists:
                            self.outcome_buffer.append(outcome)
                            self.logger.info(f"Outcome added to buffer. Buffer size: {len(self.outcome_buffer)}")
                            self.logger.debug(f"Outcome added to buffer: {outcome}")
                        else:
                            self.logger.info(f"Outcome with key {outcome_key} already exists in Redis, skipping.")
                    else:
                        self.logger.warning(f"No outcome generated for event: {transformed_event['eventId']}")
                else:
                    self.logger.info(f"Skipping outcome generation for ERROR event: {transformed_event['eventId']}")
            else:
                self.logger.info(f"Event with key {event_key} already exists in Redis, skipping.")
        else:
            self.logger.warning(f"Event transformation failed for: {event_data}")
        
        if len(self.event_buffer) >= self.buffer_size or time.time() - self.last_flush_time > self.flush_interval:
            self.logger.info(f"Buffer size reached or flush interval exceeded. Flushing buffers.")
            return await self.flush_buffer()
        return {'success': True, 'message': 'Event buffered'}

    async def flush_buffer(self) -> Dict[str, Any]:
        """
        Flush the event and outcome buffers by performing bulk inserts.
        """
        self.logger.info(f"Flushing buffers. Events: {len(self.event_buffer)}, Outcomes: {len(self.outcome_buffer)}")
        
        event_result = {'success': True, 'message': 'No events to flush', 'inserted_count': 0}
        outcome_result = {'success': True, 'message': 'No outcomes to flush', 'inserted_count': 0}

        if self.event_buffer:
            event_result = await self.bulk_insert_events(self.event_buffer)
            self.logger.info(f"Event insert result: {event_result}")
            self.event_buffer.clear()

        if self.outcome_buffer:
            self.logger.info(f"Attempting to insert {len(self.outcome_buffer)} outcomes into MongoDB")
            self.logger.debug(f"Outcome buffer contents: {self.outcome_buffer}")
            outcome_result = await self.bulk_insert_events(self.outcome_buffer)
            self.logger.info(f"Outcome insert result: {outcome_result}")
            self.outcome_buffer.clear()
        
        self.last_flush_time = time.time()
        
        if event_result['success'] and outcome_result['success']:
            return {
                'success': True,
                'message': 'Events and outcomes inserted successfully',
                'events_inserted': event_result['inserted_count'],
                'outcomes_inserted': outcome_result['inserted_count']
            }
        else:
            error_message = f"Event insert: {event_result.get('error', 'OK')}, Outcome insert: {outcome_result.get('error', 'OK')}"
            self.logger.error(f"Error in flush_buffer: {error_message}")
            return {
                'success': False,
                'error': error_message,
                'events_inserted': event_result['inserted_count'],
                'outcomes_inserted': outcome_result['inserted_count']
            }

    async def manual_flush(self) -> Dict[str, Any]:
        """
        Manually flush the buffers regardless of their current size.
        """
        self.logger.info("Manual flush triggered.")
        return await self.flush_buffer()

    async def bulk_insert_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not events:
            self.logger.info("No events to insert in bulk_insert_events")
            return {'success': True, 'message': 'No events to insert', 'inserted_count': 0}
        
        try:
            self.logger.info(f"Attempting to bulk insert {len(events)} events/outcomes")
            for event in events[:5]:  # Log first 5 events for debugging
                self.logger.debug(f"Sample event/outcome for bulk insert: {event}")
            
            # Insert into MongoDB
            mongo_result = await self.db_helper.bulk_insert_events(events)
            self.logger.info(f"MongoDB bulk insert result: {mongo_result}")
            
            if mongo_result['success']:
                # Prepare events for Redis (exclude MongoDB-specific fields)
                redis_events = [{k: v for k, v in event.items() if k != '_id'} for event in events]
                # If MongoDB insert is successful, write to Redis
                redis_result = await self.redis_helper.bulk_set_events(redis_events)
                if redis_result['success']:
                    self.logger.info(f"Successfully wrote {len(events)} events to Redis")
                    # Log some key names for verification
                    for event in events[:5]:
                        key = event['eventId']
                        self.logger.info(f"Redis key written: {key}")
                else:
                    self.logger.warning(f"Failed to write events to Redis: {redis_result.get('error', 'Unknown error')}")
            
            return {**mongo_result, 'inserted_count': len(events)}
        except Exception as e:
            self.logger.error(f"Error in bulk_insert_events: {str(e)}", exc_info=True)
            return {'success': False, 'error': str(e), 'inserted_count': 0}

    async def transform_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform the event data into the required format.
        """
        try:
            event_name = event_data.get('eventName')
            event_status = event_data.get('eventStatus')

            # Generate eventId based on eventName, eventStatus, and a UUID
            event_id = f"EVT#{event_name}#{event_status}#{uuid.uuid4()}"

            event_data['eventId'] = event_id
            event_data['type'] = 'event'
            event_data['timestamp'] = datetime.now(timezone.utc).isoformat()

            return event_data
        except Exception as e:
            self.logger.error(f"Error in transform_event: {str(e)}")
            return None

    async def insert_event_outcome(self, event_data: Dict[str, Any], existing_event_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate an outcome for a single event.
        """
        try:
            event_name = event_data['eventName']
            event_status = event_data['eventStatus']
            business_date = event_data['businessDate']
            event_time = datetime.fromisoformat(event_data['eventTime'])

            self.logger.info(f"Generating outcome for event: {event_name}, status: {event_status}, date: {business_date}")

            previous_events = []
            expectations = []
            expectation = None
            sla = None
            slo = None
            sequence_number = 0

            # Separate events by type: expectations, events, and outcomes
            for existing_event in existing_event_data:
                if existing_event['type'] == 'expectation':
                    expectations.append(existing_event)
                elif existing_event['type'] == 'event':
                    previous_events.append(existing_event)

            # If there are existing expectations, proceed to find the appropriate expectation, SLA, and SLO
            if expectations:
                metadata_response = await self.db_helper.get_event_metadata_by_name_and_status(event_name, event_status)
                event_metadata = metadata_response.get('data')

                if event_status == 'STARTED':
                    current_daily_event_count = len(previous_events)
                    if current_daily_event_count >= 1:
                        error_event_data = await self.db_helper.get_event_by_name_status_date(event_name, 'ERROR', business_date)
                        error_event_data = error_event_data.get('data', [])
                        if error_event_data:
                            current_daily_event_count -= len(error_event_data)
                elif event_status == 'SUCCESS':
                    self.logger.info(f"Generating outcome for SUCCESS event: {event_name}, date: {business_date}")
                    current_daily_event_count = len(previous_events)
                else:
                    return None

                sequence_number = current_daily_event_count + 1
                matching_occurrence = next(
                    (occurrence for occurrence in event_metadata.get('daily_occurrences', []) if
                     occurrence.get('sequence') == sequence_number),
                    None
                )

                if matching_occurrence:
                    sla = matching_occurrence.get('sla', None)
                    slo = matching_occurrence.get('slo', None)

                expectation = next(
                    (exp for exp in expectations if exp.get('sequence') == sequence_number),
                    None
                )

            outcome = self.determine_outcome(event_data, business_date, event_name, event_status, sequence_number, event_time, expectation, slo, sla)
            self.logger.info(f"Generated outcome: {outcome}")
            return outcome
        except Exception as e:
            self.logger.error(f"Error in insert_event_outcome: {str(e)}", exc_info=True)
            return None

    def determine_outcome(self, event_data: Dict[str, Any], business_date, event_name, event_status, sequence_number, event_time, expectation, slo, sla):
        slo_time = None
        sla_time = None
        slo_delta = None
        sla_delta = None

        if expectation is None:
            self.logger.info(f'No expectation found for {event_name} with status {event_status} on {business_date}')
            expected_time = None
            str_delta = ''
            outcome_status = 'NEW'
        else:
            expected_time = datetime.fromisoformat(expectation['expectedArrival'])
            if expected_time.tzinfo is None:
                expected_time = expected_time.replace(tzinfo=timezone.utc)

            delta = event_time - expected_time

            if slo and sla:
                if slo.get('status') == 'active':
                    slo_time_return = DateTimeUtils.t_plus_to_iso(business_date, slo.get('time'))
                    slo_time = datetime.fromisoformat(slo_time_return)
                    if slo_time.tzinfo is None:
                        slo_time = slo_time.replace(tzinfo=timezone.utc)
                    slo_delta = slo_time - expected_time

                if sla.get('status') == 'active':
                    sla_time_return = DateTimeUtils.t_plus_to_iso(business_date, sla.get('time'))
                    sla_time = datetime.fromisoformat(sla_time_return)
                    if sla_time.tzinfo is None:
                        sla_time = sla_time.replace(tzinfo=timezone.utc)
                    sla_delta = sla_time - expected_time

            outcome_status = 'LATE'  # Default outcome status

            if delta <= timedelta(minutes=10):
                outcome_status = 'ON_TIME'
            elif slo_delta is not None and delta <= slo_delta:
                outcome_status = 'MEETS_SLO'
            elif sla_delta is not None and delta <= sla_delta:
                outcome_status = 'MEETS_SLA'

            str_delta = str(delta.total_seconds())

        outcome_data = {
            'type': 'outcome',
            'eventId': event_data['eventId'],  # Use the same eventId as the corresponding event
            'eventName': event_name,
            'eventStatus': event_status,
            'sequence': sequence_number,
            'businessDate': business_date,
            'eventTime': event_time.isoformat(),
            'expectedTime': expected_time.isoformat() if expected_time else None,
            'sloTime': slo_time.isoformat() if slo_time else None,
            'slaTime': sla_time.isoformat() if sla_time else None,
            'delta': str_delta if str_delta else None,
            'outcomeStatus': outcome_status,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        return outcome_data

    async def get_event_by_name_status_date(self, event_name: str, event_status: str, business_date: str) -> List[Dict[str, Any]]:
        return await self.db_helper.get_event_by_name_status_date(event_name, event_status, business_date)

    async def query_events_by_date(self, business_date: str) -> Dict[str, Any]:
        return await self.db_helper.query_events_by_date(business_date)

    async def get_event_metadata_by_name_and_status(self, event_name: str, event_status: str) -> Dict[str, Any]:
        return await self.db_helper.get_event_metadata_by_name_and_status(event_name, event_status)

