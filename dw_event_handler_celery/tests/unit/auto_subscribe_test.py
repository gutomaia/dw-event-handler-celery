from unittest import TestCase
from unittest.mock import patch

from dw_events.tests.auto_subscribe_spec import AutoSubscribeSpec
from dw_events.task import autosubcribe
from dw_events.ports import EventSubscriber
from dw_events.adapters import BasicSubscriber
import inject


class AutoSubscribeTest(AutoSubscribeSpec, TestCase):
    def setUp(self) -> None:
        self.ports = []
        self.subscriber = BasicSubscriber()
        inject.configure(
            lambda binder: binder.bind(EventSubscriber, self.subscriber),
            clear=True,
        )
        self.get_ports_patched = patch(
            'dw_events.task.get_ports', wraps=self.get_ports
        )
        self.get_ports_mock = self.get_ports_patched.start()

    def tearDown(self) -> None:
        self.get_ports_patched.stop()

    def get_ports(self):
        return self.ports

    def given_port(self, port):
        self.ports.append(('any', port))

    def when_autosubscribe(self):
        autosubcribe()

    def assert_is_subscribed(self, event_class, handler):
        self.assertIn(handler, self.subscriber.get_subscribers(event_class))

    def assert_no_subscriptions(self):
        actual = len(self.subscriber.subscriptions)
        self.assertEqual(0, actual)
