"""
Event Services Batch Processing Module

This module provides batch processing capabilities for event services,
including event insertion, transformation, and outcome generation.

Updated: [Current Date]
Changes:
- Added type hints for improved code clarity and static type checking
- Implemented batch processing for event insertions and outcome generations
- Improved error handling and logging
- Optimized database queries with batch operations
- Ensured outcomes are inserted for each non-ERROR event
- Added more detailed logging for buffer flushing
- Added transform_event method to fix AttributeError
- Enhanced logging in generate_event_outcome method
- Added detailed logging in insert_event method
- Improved logging in bulk_insert_events method
- Modified flush_buffer to return counts of inserted events and outcomes
- Updated insert_event method to align with event_services.py logic
- Updated import statement to use absolute import for DateTimeUtils
"""

import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Tuple
from utils.common import DateTimeUtils
from collections import defaultdict
import logging

class EventServicesBatch:
    def __init__(self, db_helper: Any, logger: logging.Logger):
        self.db_helper = db_helper
        self.logger = logger
        self.event_buffer: List[Dict[str, Any]] = []
        self.outcome_buffer: List[Dict[str, Any]] = []
        self.buffer_size = 100  # Adjust this value based on your requirements

    def insert_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert a single event and generate its outcome. This method buffers events and performs bulk insert when the buffer is full.
        """
        self.logger.info(f"Inserting event: {event_data}")
        transformed_event = self.transform_event(event_data)
        if transformed_event:
            self.event_buffer.append(transformed_event)
            self.logger.info(f"Event added to buffer. Buffer size: {len(self.event_buffer)}")
            
            if transformed_event['eventStatus'] != 'ERROR':
                # Generate and buffer the outcome
                existing_event_data = self.db_helper.get_event_by_name_status_date(
                    transformed_event['eventName'],
                    transformed_event['eventStatus'],
                    transformed_event['businessDate']
                ).get('data', [])
                outcome = self.insert_event_outcome(transformed_event, existing_event_data)
                if outcome:
                    self.outcome_buffer.append(outcome)
                    self.logger.info(f"Outcome added to buffer. Buffer size: {len(self.outcome_buffer)}")
                else:
                    self.logger.warning(f"No outcome generated for event: {transformed_event['eventId']}")
            else:
                self.logger.info(f"Skipping outcome generation for ERROR event: {transformed_event['eventId']}")
        else:
            self.logger.warning(f"Event transformation failed for: {event_data}")
        
        if len(self.event_buffer) >= self.buffer_size:
            self.logger.info(f"Buffer size reached. Flushing buffers.")
            return self.flush_buffer()
        return {'success': True, 'message': 'Event buffered'}

    def flush_buffer(self) -> Dict[str, Any]:
        """
        Flush the event and outcome buffers by performing bulk inserts.
        """
        self.logger.info(f"Flushing buffers. Events: {len(self.event_buffer)}, Outcomes: {len(self.outcome_buffer)}")
        
        event_result = {'success': True, 'message': 'No events to flush', 'inserted_count': 0}
        outcome_result = {'success': True, 'message': 'No outcomes to flush', 'inserted_count': 0}

        if self.event_buffer:
            event_result = self.bulk_insert_events(self.event_buffer)
            self.logger.info(f"Event insert result: {event_result}")
            self.event_buffer.clear()

        if self.outcome_buffer:
            outcome_result = self.bulk_insert_events(self.outcome_buffer)
            self.logger.info(f"Outcome insert result: {outcome_result}")
            self.outcome_buffer.clear()
        
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

    def bulk_insert_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Insert multiple events in bulk.
        """
        if not events:
            self.logger.info("No events to insert in bulk_insert_events")
            return {'success': True, 'message': 'No events to insert', 'inserted_count': 0}
        
        try:
            self.logger.info(f"Attempting to bulk insert {len(events)} events/outcomes")
            for event in events[:5]:  # Log first 5 events for debugging
                self.logger.debug(f"Sample event/outcome for bulk insert: {event}")
            
            result = self.db_helper.bulk_insert_events(events)
            self.logger.info(f"Bulk insert result: {result}")
            return {**result, 'inserted_count': len(events)}
        except Exception as e:
            self.logger.error(f"Error in bulk_insert_events: {str(e)}", exc_info=True)
            return {'success': False, 'error': str(e), 'inserted_count': 0}

    def transform_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform the event data into the required format.
        """
        try:
            event_name = event_data.get('eventName')
            event_status = event_data.get('eventStatus')

            event_id = f"EVT#{event_name}#{event_status}#{uuid.uuid4()}"
            event_data['eventId'] = event_id
            event_data['type'] = 'event'
            event_data['timestamp'] = datetime.now(timezone.utc).isoformat()

            return event_data
        except Exception as e:
            self.logger.error(f"Error in transform_event: {str(e)}")
            return None

    def insert_event_outcome(self, event_data: Dict[str, Any], existing_event_data: List[Dict[str, Any]]) -> Dict[str, Any]:
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
                metadata_response = self.db_helper.get_event_metadata_by_name_and_status(event_name, event_status)
                event_metadata = metadata_response.get('data')

                if event_status == 'STARTED':
                    current_daily_event_count = len(previous_events)
                    if current_daily_event_count >= 1:
                        error_event_data = self.db_helper.get_event_by_name_status_date(event_name, 'ERROR', business_date).get('data', [])
                        if error_event_data:
                            current_daily_event_count -= len(error_event_data)
                elif event_status == 'SUCCESS':
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

            outcome = self.determine_outcome(business_date, event_name, event_status, sequence_number, event_time, expectation, slo, sla)
            self.logger.info(f"Generated outcome: {outcome}")
            return outcome
        except Exception as e:
            self.logger.error(f"Error in insert_event_outcome: {str(e)}", exc_info=True)
            return None

    def determine_outcome(self, business_date, event_name, event_status, sequence_number, event_time, expectation, slo, sla):
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
            'eventId': f"OUT#{event_name}#{event_status}#{uuid.uuid4()}",
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

    # ... (rest of the methods remain unchanged)