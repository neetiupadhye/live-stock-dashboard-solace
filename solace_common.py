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

# The set of stocks the publisher polls/publishes and the dashboard
# lets the user pick between. Add/remove tickers here and both sides
# pick it up automatically.
AVAILABLE_TICKERS = ["D05.SI", "O39.SI", "AAPL", "MSFT"]

# Human-friendly display info per ticker, keyed the same way. Falls
# back to sensible defaults in dashboard.py if a ticker isn't listed.
TICKER_INFO = {
    "D05.SI": {"name": "DBS GROUP HOLDINGS", "exchange": "SGX", "currency": "SGD"},
    "O39.SI": {"name": "OCBC BANK", "exchange": "SGX", "currency": "SGD"},
    "AAPL": {"name": "APPLE INC", "exchange": "NASDAQ", "currency": "USD"},
    "MSFT": {"name": "MICROSOFT CORP", "exchange": "NASDAQ", "currency": "USD"},
}


def topic_for_ticker(ticker_symbol):
    """
    The topic a given ticker's ticks are published/subscribed on.
    One topic per ticker (no sequence number in the path) so the
    subscriber can subscribe to exactly one stock at a time.
    """
    return f"{TOPIC_PREFIX}/python/stocks/{ticker_symbol}"


# Separate topic namespace used to ask the publisher to replay a
# ticker's day-so-far history on demand — used when the dashboard
# switches to a stock it wasn't already subscribed to, so it gets the
# same "backfill then live" experience as the very first stock shown.
BACKFILL_REQUEST_TOPIC_PREFIX = f"{TOPIC_PREFIX}/python/backfill-request"


def backfill_request_topic(ticker_symbol):
    return f"{BACKFILL_REQUEST_TOPIC_PREFIX}/{ticker_symbol}"


def topic_for_news(ticker_symbol):
    """
    The topic a given ticker's news articles are published/subscribed
    on. Separate namespace from price ticks (different message shape,
    much lower frequency) but same one-topic-per-ticker convention, so
    the subscriber can subscribe to just the news for whichever stock
    it's currently showing.
    """
    return f"{TOPIC_PREFIX}/python/news/{ticker_symbol}"


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
