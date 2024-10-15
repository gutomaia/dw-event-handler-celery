from dw_events.adapters import AbstractEventHandler
from dw_events.adapters import BasicSubscriber
from dw_events.serializer import unserialize_class, unserialize_event
from dw_events.task import is_event_function, is_event_task
from celery import Celery
from celery import shared_task


celery_app = Celery(
    'tasks',
    broker='memory://',
    result_backend='cache+memory://',
    # broker='redis://localhost:6379/0',
    # backend='redis://localhost:6379/0',
)


@shared_task(bind=True)
def run_function_handle(self, serialized_handler, serialized_event):
    handler = unserialize_class(serialized_handler)
    event = unserialize_event(serialized_event)
    handler(event)


@shared_task(bind=True)
def run_class_handle(self, serialized_handler, serialized_event):
    handler = unserialize_class(serialized_handler)
    event = unserialize_event(serialized_event)
    instance = handler(event)
    instance.run()


class CeleryEventHandler(AbstractEventHandler):
    def __init__(self):
        super().__init__(BasicSubscriber())

    # def handle(self, event: Event):
    #     handlers = self.subscriber.get_subscribers(event.__class__)
    #     serialized_event = serialize_event(event)
    #     for handle in handlers:
    #         serialized_handler = serialize_class(handle)
    #         self.queue_handling(serialized_handler, serialized_event)

    def queue_handling(self, serialized_handler, serialized_event):
        handler = unserialize_class(serialized_handler)
        if is_event_function(handler):
            run_function_handle.delay(serialized_handler, serialized_event)
        elif is_event_task(handler):
            run_class_handle.delay(serialized_handler, serialized_event)
