from dw_core.cqrs import Event
from dw_core.adapters import Task


class AutoSubscribeSpec:
    def given_port(self, port):
        raise NotImplementedError()

    def when_autosubscribe(self):
        raise NotImplementedError()

    def assert_is_subscribed(self, event_class, handler):
        raise NotImplementedError()

    def assert_no_subscriptions(self):
        raise NotImplementedError()

    def test_autosubscribe_function(self):
        class Notification(Event):
            msg: str

        def notify(notification: Notification) -> None:
            pass

        self.given_port(notify)

        self.when_autosubscribe()

        self.assert_is_subscribed(Notification, notify)

    def test_autosubscribe_function_without_none_return(self):
        class Notification(Event):
            msg: str

        def notify(notification: Notification):
            pass

        self.given_port(notify)

        self.when_autosubscribe()

        self.assert_is_subscribed(Notification, notify)

    def test_autosubscribe_function_must_not_subscribe(
        self,
    ):
        class Notification(Event):
            msg: str

        def notify(notification: Notification) -> Notification:
            return notification

        self.given_port(notify)

        self.when_autosubscribe()

        self.assert_no_subscriptions()

    def test_autosubscribe_with_wrong_argument_type(self):
        def notify(msg: str) -> None:
            pass

        self.given_port(notify)

        self.when_autosubscribe()

        self.assert_no_subscriptions()

    def test_autosubscribe_without_argument_type(self):
        def notify(msg) -> None:
            pass

        self.given_port(notify)

        self.when_autosubscribe()

        self.assert_no_subscriptions()

    def test_autosubscribe_task(self):
        class Context(Event):
            pass

        class Job(Task):
            def __init__(self, context: Context):
                pass

        self.given_port(Job)

        self.when_autosubscribe()

        self.assert_is_subscribed(Context, Job)
