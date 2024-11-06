# events.py
from enum import Enum, auto
from typing import List, Callable, Any, Dict
from abc import ABC, abstractmethod
from typing import Optional
from functools import wraps
from multiprocessing import Lock
from collections import deque, defaultdict
from queue import Queue
import threading

#TODO Check if a description is ever used.
class DataEventType(Enum):
    IMPLICIT = "implicit"
    CODED_VALUE = "coded value"
    LITERAL_VALUE = "literal value"
    JUSTIFIED_ABSENCE = "justified absence"
    UNJUSTIFIED_ABSENCE = "unjustified absence"
    REDUNDANT_PRESENCE = "redundant presence"
    UNJUSTIFIED_PRESENCE = "unjustified presence"
    DISALLOWED_VALUE = "disallowed value"


def synchronized(member):
    """
    @synchronized decorator.

    Lock a method for synchronized access only. The lock is stored to
    the function or class instance, depending on what is available.
    """

    @wraps(member)
    def wrapper(*args, **kwargs):
        lock = vars(member).get("_synchronized_lock", None)
        result = None
        try:
            if lock is None:
                lock = vars(member).setdefault("_synchronized_lock", Lock())
            lock.acquire()
            result = member(*args, **kwargs)
            lock.release()
        except Exception as e:
            lock.release()
            raise e
        return result

    return wrapper


class DataEvent:  # Event structure

    #TODO Have event_type_val be either a string or DataEventType and parse depending on which
    def __init__(self, field_name: str, field_value: str, event_type_val: str, record_number: int):
        self.field_name = field_name
        self.field_value = field_value
        self.event_type = DataEventType(event_type_val)
        self.record_number = record_number


    #TODO Figure out what layout for __str__


class ADataEventFilter(ABC):

    def __init__(self):
        self.field_name = None
        self.field_value = None
        self.parse_event = None

    @abstractmethod
    def passes(e: DataEvent):
        raise NotImplementedError

    def __equals__(self, obj):
        if not isinstance(obj, ADataEventFilter):
            return False
        return self.field_name == obj.field_name and self.field_value == obj.field_value and self.parse_event == obj.parse_event

class NullDataEventFilter(ADataEventFilter):
    def passes(self, e: DataEvent):
        assert e is not None
        return True



class DataEventFilter(ADataEventFilter):  # Base filter class
    def __init__(self, field_name: str=None, field_value: str=None, event_type: DataEventType=None):
        self.field_name = field_name
        self.field_value = field_value
        self.event_type = event_type
        #TODO Ensure at least one is not None

    def passes(self, e: DataEvent):
        return (not self.field_name or (self.field_name == e.field_name)) and (not self.field_value or (self.field_value == e.field_value)) and (not self.event_type or (self.event_type == e.event_type))

class ADataEventSubscriber(ABC):
    """Abstract DataEventSubscriber Template"""

    def __init__(self):
        super.__init__()
        self.filter = NullDataEventFilter()
        #TODO Figure out what type for writer
        self.writer = None
        self.count = 0
        self.field_name = ""
        self.event_type = None

    @abstractmethod
    def notify(self, event: DataEvent):
        pass

class DataEventTypeByFieldCounterSubscriber(ADataEventSubscriber):
    def __init__(self, de:DataEventType, field_name:str):
        super.__init__()
        self.event_type = de
        self.field_name = field_name

    def notify(self, event: DataEvent):
        if self.event_type == event.event_type and self.field_name == event.field_name:
            self.counter += 1

class DataEventTypeCounterSubscriber(ADataEventSubscriber):
    def __init__(self, de:DataEventType):
        super.__init__()
        self.event_type = de
        self.filter = DataEventFilter(event_type=de)

    def notify(self, e: DataEvent):
        if e.event_type == self.event_type:
            self.count += 1
          
class AllDataEventStreamWriterSubscriber(ADataEventSubscriber):
    def __init__(self, writer, delim='\t'):
        super().__init__()
        self.writer = writer
        self.delim = delim

    def notify(self, data_event:DataEvent):
        try:
            # Retrieve and write each field with delimiter
            self.writer.write(f"{data_event.event_type or 'None'}{self.delim}")
            self.writer.write(f"{data_event.field_name or 'None'}{self.delim}")
            self.writer.write(f"{data_event.field_value or 'None'}{self.delim}")
            self.writer.write(f"{data_event.record_number}\n")
            
            # Flush after each write to ensure data is immediately written
            self.writer.flush()
        except IOError as e:
            print(f"IO error occurred when writing AllDataEventStreamWriterSubscriber: {e}")


class DataEventTypeByFieldStreamWriterSubscriber(ADataEventSubscriber):
    def __init__(self, de:DataEventType, field_name:str, writer, delim: str):
        super().__init__()
        self.writer = writer
        self.delim = delim
        self.event_type = de
        self.field_name = field_name

    def notify(self, data_event: DataEvent):
        event_type = data_event.event_type
        event_field_name = data_event.field_name

        # Check conditions before writing
        if event_type is not None and event_type == self.event_type and event_field_name is not None and event_field_name == self.field_name:
            try:
                # Write the event data fields, with fallback to 'None' if a field is None
                self.writer.write(f"{event_type}{self.delim}")
                self.writer.write(f"{event_field_name}{self.delim}")
                self.writer.write(f"{data_event.field_value or 'None'}{self.delim}")
                self.writer.write(f"{data_event.record_number}\n")
                
                # Ensure data is written immediately
                self.writer.flush()
            except IOError as e:
                print(f"IO error occurred when writing DataEventTypeByFieldStreamWriterSubscriber: {e}")

class DataEventTypeStreamWriterSubscriber(ADataEventSubscriber):
    def __init__(self, de:DataEventType, writer, delim: str='\t'):
        super().__init__()
        self.writer = writer
        self.delim = delim
        self.event_type = de
        self.filter = DataEventFilter(event_type=de)

    def notify(self, data_event):
        try:
            # Write data event type or 'null' if None
            self.writer.write(f"{data_event.get_data_event_type() or 'null'}{self.delim}")
            
            # Write field name or 'null' if None
            self.writer.write(f"{data_event.get_field_name() or 'null'}{self.delim}")
            
            # Write field value or 'null' if None
            self.writer.write(f"{data_event.get_field_value() or 'null'}{self.delim}")
            
            # Write record number and a new line
            self.writer.write(f"{data_event.get_record_number()}\n")
            
            # Flush to ensure data is written immediately
            self.writer.flush()
        except IOError as e:
            print(f"IO error occurred when writing DataEventTypeStreamWriterSubscriber: {e}")



#TODO Decide if I want to stay with static or go to class methods
class DataEventMessageBoard:
    """
    Message board that manages events and their subscribers, supporting subscriptions
    and broadcasting of events to matching subscribers.
    """
    
    queue = None
    filter_subscriber_map = None
    
    @staticmethod
    @synchronized
    def start():
        if DataEventMessageBoard.queue is None:
            DataEventMessageBoard.queue = deque(maxlen=50)  # deque to hold up to 50 events
            DataEventMessageBoard.filter_subscriber_map = defaultdict(list)
    
    @staticmethod
    def publish(event: DataEvent):
        DataEventMessageBoard.add_event_to_queue(event)
        DataEventMessageBoard.broadcast_event_to_subscribers(event)
    
    @staticmethod
    @synchronized
    def add_event_to_queue(event: DataEvent):
        # Enqueue event, and remove the oldest if the queue is full
        DataEventMessageBoard.queue.append(event)
    
    @staticmethod
    def broadcast_event_to_subscribers(event: DataEvent):
        for event_filter, subscribers in DataEventMessageBoard.filter_subscriber_map.items():
            if event_filter.passes(event):
                for subscriber in subscribers:
                    subscriber.notify(event)
    
    @staticmethod
    def subscribe_all(subscriber: ADataEventSubscriber):
        DataEventMessageBoard.subscribe(subscriber, NullDataEventFilter())
    
    @staticmethod
    @synchronized
    def subscribe(subscriber: ADataEventSubscriber, event_filter: ADataEventFilter):
        # Add subscriber to filter's list
        DataEventMessageBoard.filter_subscriber_map[event_filter].append(subscriber)
        
        # Notify subscriber of all past events that pass the filter
        for past_event in DataEventMessageBoard.queue:
            if event_filter.passes(past_event):
                subscriber.notify(past_event)
    
    @staticmethod
    def unsubscribe(subscriber: ADataEventSubscriber, event_filter: ADataEventFilter):
        # Remove subscriber from filter's list
        if event_filter in DataEventMessageBoard.filter_subscriber_map:
            DataEventMessageBoard.filter_subscriber_map[event_filter].remove(subscriber)

