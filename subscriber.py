"""
subscriber.py

Standalone subscriber: connects to the Solace broker and subscribes
to exactly ONE stock's price topic and ONE stock's news topic at a
time (always the same stock for both), writing every received data
point into data_store or news_store depending on what kind of message
it is. Which stock that is can be changed at runtime via
switch_ticker() — that's what lets the dashboard's dropdown swap the
live feed when the user picks a different stock.

Switching (or the initial connect) also fires a backfill request at
the publisher, asking it to replay that ticker's day-so-far history
onto its normal data topic before live ticks resume — so picking a
stock always gets the same "full history, then live" treatment the
very first ticker gets, instead of only building up a chart from
whatever ticks happen to arrive after you switched to it.

This is what feeds the dashboard, so it needs to run in the same
process as dashboard.py (see main.py) since they share the in-memory
data_store and this module's receiver. It does NOT need to run on the
same machine as the publisher — only the same Solace broker.

Run directly (useful for testing the subscriber on its own, without
the dashboard) — it will just sit on the default ticker forever:

    python3 subscriber.py
"""

import json
import threading
import time

from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.resources.topic import Topic
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.receiver.inbound_message import InboundMessage

from data_store import data_store
from news_store import news_store
from solace_common import (
    AVAILABLE_TICKERS,
    topic_for_ticker,
    topic_for_news,
    backfill_request_topic,
    build_messaging_service,
    attach_service_listeners,
)

# The live Direct Receiver and which topics it's currently subscribed
# to (both price and news, for the same ticker), plus a small Direct
# Publisher used only to send backfill requests. Set once
# run_subscriber() starts; switch_ticker() below mutates these under
# _subscription_lock, since it can be called from the dashboard's
# callback thread while the receiver is running.
_receiver = None
_current_topic = None
_current_news_topic = None
_backfill_publisher = None
_backfill_message_builder = None
_subscription_lock = threading.Lock()


class MessageHandlerImpl(MessageHandler):
    def on_message(self, message: "InboundMessage"):
        try:
            payload = message.get_payload_as_string() or message.get_payload_as_bytes()
            if isinstance(payload, bytearray):
                payload = payload.decode()

            data = json.loads(payload)
            # The ticker comes from the message payload itself, not
            # the topic — this keeps the stores correct even for the
            # brief window during a switch_ticker() call where both
            # the old and new subscriptions might momentarily overlap.
            ticker = data["ticker"]

            if "articles" in data:
                # News message: {"ticker", "articles", "fetched_at"}
                news_store.set(ticker, data["articles"], data.get("fetched_at"))
                print(f"Stored [{ticker}] news: {len(data['articles'])} article(s)")
            else:
                # Price tick: {"ticker", "date", "current", "is_live", ...}
                data_store.add(ticker, data["date"], data["current"], data.get("is_live", False))
                status = "LIVE" if data.get("is_live", False) else "closed"
                print(f"Stored [{ticker}] ({status}): {data['date']} -> {data['current']}")

        except Exception as e:
            print(f"Error processing message: {e}")


def _request_backfill(ticker):
    """
    Ask the publisher to replay `ticker`'s day-so-far bars onto its
    normal data topic (see BackfillRequestHandler in publisher.py).
    Best-effort: if there's no publisher listening, this just silently
    does nothing and the chart builds up from live ticks only, same
    as before this feature existed.
    """
    if _backfill_publisher is None:
        return
    try:
        payload = json.dumps({"ticker": ticker})
        outbound_message = _backfill_message_builder.build(payload)
        _backfill_publisher.publish(
            destination=Topic.of(backfill_request_topic(ticker)),
            message=outbound_message,
        )
        print(f"Requested backfill for {ticker}")
    except PubSubPlusClientError as exception:
        print(f"Failed to request backfill for {ticker}: {exception}")


def switch_ticker(new_ticker):
    """
    Re-point the live subscription at a different ticker's topic and
    ask the publisher to replay its history. Called from the dashboard
    when the user picks a different stock in the dropdown — runs on
    the dashboard's thread, not the subscriber's, so it's guarded by
    _subscription_lock.

    Returns True if the subscription is (now) on new_ticker's topic,
    False if the receiver isn't up yet.
    """
    global _current_topic, _current_news_topic

    with _subscription_lock:
        if _receiver is None:
            return False

        new_topic = topic_for_ticker(new_ticker)
        new_news_topic = topic_for_news(new_ticker)
        if new_topic == _current_topic:
            return True  # already subscribed to this one

        try:
            # Subscribe to both new topics BEFORE clearing/requesting,
            # so we can't miss the backfill reply (price or news)
            # that's about to come back on either of them.
            _receiver.add_subscription(TopicSubscription.of(new_topic))
            _receiver.add_subscription(TopicSubscription.of(new_news_topic))

            # Wipe any stale price data left over from a previous visit
            # to this ticker — the backfill reply is about to resend
            # the full day-so-far, so keeping old points around would
            # just duplicate them ahead of the fresh ones. News is left
            # alone (not cleared): a stale-but-present headline is a
            # better sidebar experience than a flash of "no news" while
            # the fresh fetch is in flight, and the incoming news
            # message replaces it wholesale as soon as it arrives.
            data_store.clear(new_ticker)
            _request_backfill(new_ticker)

            if _current_topic is not None:
                _receiver.remove_subscription(TopicSubscription.of(_current_topic))
            if _current_news_topic is not None:
                _receiver.remove_subscription(TopicSubscription.of(_current_news_topic))
            _current_topic = new_topic
            _current_news_topic = new_news_topic
            print(f"Switched live subscription to: {new_topic} and {new_news_topic}")
            return True
        except PubSubPlusClientError as exception:
            print(f"Failed to switch subscription to {new_topic}: {exception}")
            return False


def run_subscriber(initial_ticker=None):
    global _receiver, _current_topic, _current_news_topic, _backfill_publisher, _backfill_message_builder

    initial_ticker = initial_ticker or AVAILABLE_TICKERS[0]
    initial_topic = topic_for_ticker(initial_ticker)
    initial_news_topic = topic_for_news(initial_ticker)

    messaging_service = build_messaging_service()
    attach_service_listeners(messaging_service)

    direct_receiver = (
        messaging_service.create_direct_message_receiver_builder()
        .with_subscriptions([TopicSubscription.of(initial_topic), TopicSubscription.of(initial_news_topic)])
        .build()
    )

    backfill_publisher = messaging_service.create_direct_message_publisher_builder().build()
    backfill_message_builder = messaging_service.message_builder().with_property("purpose", "backfill-request")

    try:
        direct_receiver.start()
        direct_receiver.receive_async(MessageHandlerImpl())
        backfill_publisher.start()

        with _subscription_lock:
            _receiver = direct_receiver
            _current_topic = initial_topic
            _current_news_topic = initial_news_topic
            _backfill_publisher = backfill_publisher
            _backfill_message_builder = backfill_message_builder

        if direct_receiver.is_running():
            print(f"Subscribed to: {initial_topic} and {initial_news_topic}\nReady to receive\n")

        # Same "backfill then live" treatment as switch_ticker() gives
        # every subsequent stock, applied to the one we start on too.
        # (This also triggers a fresh news fetch — see
        # BackfillRequestHandler in publisher.py — on top of the
        # startup news publish the publisher does on its own.)
        _request_backfill(initial_ticker)

        # The Solace API delivers messages on its own callback thread,
        # so this loop just needs to keep the process/thread alive
        # until it's told to stop. switch_ticker() can be called at
        # any point from another thread while this loop runs.
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDisconnecting Messaging Service")
    except PubSubPlusClientError as exception:
        print(f"Received a PubSubPlusClientException: {exception}")
    finally:
        with _subscription_lock:
            _receiver = None
            _current_topic = None
            _current_news_topic = None
            _backfill_publisher = None
            _backfill_message_builder = None
        print("Terminating Receiver")
        direct_receiver.terminate()
        backfill_publisher.terminate()
        print("Disconnecting Messaging Service")
        messaging_service.disconnect()


if __name__ == "__main__":
    run_subscriber()
