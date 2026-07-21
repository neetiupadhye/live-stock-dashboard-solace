"""
solace_common.py

Shared Solace connection setup used by both publisher.py and
subscriber.py. Keeping this in one place means both sides agree on
the same broker details and topic naming, whether they run in the
same process or on completely separate machines.

Connection details come from environment variables, so you can point
publisher.py and subscriber.py at the same broker even when they run
on different hosts:

    SOLACE_HOST      e.g. tcp://mybroker.example.com:55555
    SOLACE_VPN
    SOLACE_USERNAME
    SOLACE_PASSWORD

If unset, they fall back to a local PubSub+ software broker, which is
only reachable if publisher/subscriber are on the same machine/network
as that broker.
"""

import os

from solace.messaging.messaging_service import (
    MessagingService,
    ReconnectionListener,
    ReconnectionAttemptListener,
    ServiceInterruptionListener,
    RetryStrategy,
    ServiceEvent,
)

# Both publisher and subscriber need to agree on this to find each
# other's messages.
TOPIC_PREFIX = "solace/samples"


def get_broker_props():
    return {
        "solace.messaging.transport.host": os.environ.get("SOLACE_HOST") or "tcp://localhost:55554",
        "solace.messaging.service.vpn-name": os.environ.get("SOLACE_VPN") or "default",
        "solace.messaging.authentication.scheme.basic.username": os.environ.get("SOLACE_USERNAME") or "admin",
        "solace.messaging.authentication.scheme.basic.password": os.environ.get("SOLACE_PASSWORD") or "admin",
    }


def build_messaging_service():
    """Build and (blocking) connect a MessagingService using the shared broker props."""
    messaging_service = (
        MessagingService.builder()
        .from_properties(get_broker_props())
        .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3))
        .build()
    )
    messaging_service.connect()
    return messaging_service


class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent):
        print("\non_reconnected")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

    def on_reconnecting(self, e: "ServiceEvent"):
        print("\non_reconnecting")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

    def on_service_interrupted(self, e: "ServiceEvent"):
        print("\non_service_interrupted")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")


def attach_service_listeners(messaging_service):
    """Wire up the standard reconnection/interruption logging listeners."""
    handler = ServiceEventHandler()
    messaging_service.add_reconnection_listener(handler)
    messaging_service.add_reconnection_attempt_listener(handler)
    messaging_service.add_service_interruption_listener(handler)
    return handler
