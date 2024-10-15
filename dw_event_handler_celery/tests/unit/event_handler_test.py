from typing import Callable
from unittest import TestCase
from unittest.mock import patch
from dw_core.cqrs import Event
from dw_events.tests.event_handler_spec import EventHandlerSpec, execute
from dw_event_handler_celery.handler import CeleryEventHandler, celery_app


class CeleryEagerEventHandlerTest(EventHandlerSpec, TestCase):
    def setUp(self) -> None:
        celery_app.conf.update(
            broker_url='memory://',
            result_backend='cache+memory://',
            task_always_eager=True,
            task_eager_propagates=True,
        )

        self.event_handler = CeleryEventHandler()
        self.patcher = patch(
            'dw_events.tests.event_handler_spec.execute', wraps=execute
        )
        self.mock = self.patcher.start()

    def tearDown(self) -> None:
        self.patcher.stop()

    def given_subscription(
        self, event_class: Event, executer: Callable[[Event], None]
    ):
        self.event_handler.subscriber.subscribe(event_class, executer)

    def when_event_handler(self, event: Event):
        self.event_handler.handle(event)

    def assert_handler_called(self):
        self.mock.assert_called()


class CeleryRedisEventHandlerTest(CeleryEagerEventHandlerTest):
    def setUp(self) -> None:
        super().setUp()
        celery_app.conf.update(
            broker_url='memory://',
            result_backend='cache+memory://',
            task_always_eager=True,
            task_eager_propagates=True,
        )
