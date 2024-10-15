from typing import Callable
from dw_core.cqrs import Event
from dw_core.ports import BackgroundTask
from dw_core.adapters import Task


def execute(event):
    pass


class Message(Event):
    msg: str


def handler(event: Message):
    execute(event)


class BackgroundJob(BackgroundTask):
    def __init__(self, event: Message):
        self.event = event

    def run(self):
        execute(self.event)


class Job(Task):
    def __init__(self, event: Message):
        self.event = event

    def run(self):
        execute(self.event)


class EventHandlerSpec:
    def given_subscription(
        self, event_class: type[Event], executer: Callable[[Event], None]
    ):
        raise NotImplementedError()

    def when_event_handler(self, event: Event):
        raise NotImplementedError()

    def assert_handler_called(self):
        raise NotImplementedError()

    def test_event_handler_function(self):
        self.given_subscription(Message, handler)

        self.when_event_handler(Message(msg='Hello World'))

        self.assert_handler_called()

    def test_event_handler_class(self):
        self.given_subscription(Message, BackgroundJob)

        self.when_event_handler(Message(msg='Hello World'))

        self.assert_handler_called()

    def test_event_handler_class2(self):
        self.given_subscription(Message, Job)

        self.when_event_handler(Message(msg='Hello World'))

        self.assert_handler_called()
