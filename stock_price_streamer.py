## Goal: Publisher + Subscriber (live yfinance version)
import os
import time

# Import Solace Python  API modules
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener, FailedPublishEvent
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.config.solace_properties.message_properties import APPLICATION_MESSAGE_ID
from solace.messaging.resources.topic import Topic
from solace.messaging.receiver.inbound_message import InboundMessage

import yfinance as yf
import json #to parse the data into readable objects

import threading
from collections import deque
from data_store import data_store

TICKER = "D05.SI"   # DBS Group Holdings, listed on SGX — swapped from AAPL so the market is actually open right now (SGX trades 9am-5pm SGT); change back to "AAPL" once you're testing during US market hours
TOPIC_PREFIX = "solace/samples"
POLL_INTERVAL_SECONDS = 15   # 1-minute bars can't update faster than once a minute anyway; 15s just catches the new bar promptly without hammering the endpoint
SHUTDOWN = False


def get_latest_quote(ticker_symbol):
    """
    Fetch the most recent price tick for a ticker via yfinance.

    NOTE: we intentionally use history(period="1d", interval="1m") here
    instead of Ticker.fast_info. fast_info hits a lightweight Yahoo quote
    endpoint that gets cached upstream and can return the exact same
    price for many polls in a row if you poll faster than that cache
    refreshes. Pulling the latest 1-minute bar avoids that.
    """
    ticker_obj = yf.Ticker(ticker_symbol)
    bars = ticker_obj.history(period="1d", interval="1m")
    if bars.empty:
        raise RuntimeError(f"No intraday data returned for {ticker_symbol} (market may be closed)")

    last_bar = bars.iloc[-1]
    bar_timestamp = bars.index[-1]   # the actual market timestamp this price applies to

    return {
        "date": bar_timestamp.isoformat(),   # real market time the bar represents, not the time we happened to poll
        "ticker": ticker_symbol,
        "current": float(last_bar["Close"]),
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S"),   # debug only: wall-clock time we polled, kept to show how much lag exists
    }


# Handle received messages
class MessageHandlerImpl(MessageHandler):
    def on_message(self, message: 'InboundMessage'):
        try:
            global SHUTDOWN #global tells the code it is the global variable
            if "quit" in message.get_destination_name():
                print("QUIT message received, shutting down.")
                SHUTDOWN = True
                return
            # Check if the payload is a String or Byte, decode if its the later
            payload = message.get_payload_as_string() or message.get_payload_as_bytes()
            if isinstance(payload, bytearray):
                payload = payload.decode()

            data = json.loads(payload)
            data_store.add(data["date"], data["current"]) #to parse and store instead of just printing the message

            print(f"Stored: {data['date']} -> {data['current']}")

        except Exception as e:
            print(f"Error processing message: {e.__traceback__}")

# Inner classes for error handling
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

class PublisherErrorHandling(PublishFailureListener):
    def on_failed_publish(self, e: "FailedPublishEvent"):
        print("on_failed_publish")

def run_streamer():
    # Broker Config
    broker_props = {
        "solace.messaging.transport.host": os.environ.get('SOLACE_HOST') or "tcp://localhost:55554",
        "solace.messaging.service.vpn-name": os.environ.get('SOLACE_VPN') or "default",
        "solace.messaging.authentication.scheme.basic.username": os.environ.get('SOLACE_USERNAME') or "admin",
        "solace.messaging.authentication.scheme.basic.password": os.environ.get('SOLACE_PASSWORD') or "admin"
        }

    # Build A messaging service with a reconnection strategy of 20 retries over an interval of 3 seconds
    # Note: The reconnections strategy could also be configured using the broker properties object
    messaging_service = MessagingService.builder().from_properties(broker_props)\
                        .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20,3))\
                        .build()

    # Blocking connect thread
    messaging_service.connect()
    # print(f'Messaging Service connected? {messaging_service.is_connected}')

    # Error Handeling for the messaging service
    service_handler = ServiceEventHandler()
    messaging_service.add_reconnection_listener(service_handler)
    messaging_service.add_reconnection_attempt_listener(service_handler)
    messaging_service.add_service_interruption_listener(service_handler)

    # Create a direct message publisher and start it
    direct_publisher = messaging_service.create_direct_message_publisher_builder().build()
    direct_publisher.set_publish_failure_listener(PublisherErrorHandling())
    direct_publisher.set_publisher_readiness_listener

    # Blocking Start thread
    direct_publisher.start()
    # print(f'Direct Publisher ready? {direct_publisher.is_ready()}')

    # Define a Topic subscriptions
    topics = [TOPIC_PREFIX + "/python/stocks/>"]
    topics_sub = []
    for t in topics:
        topics_sub.append(TopicSubscription.of(t))

    # Prepare outbound message
    message_builder = messaging_service.message_builder() \
                    .with_application_message_id("sample_id") \
                    .with_property("application", "samples") \
                    .with_property("language", "Python") \

    try:
        print(f"Subscribed to: {topics}")
        # Build a Receiver
        direct_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(topics_sub).build()
        direct_receiver.start()
        # Callback for received messages
        direct_receiver.receive_async(MessageHandlerImpl())
        if direct_receiver.is_running():
            print("Connected and Subscribed! Ready to publish\n")
        try:
            msgSeqNum = 0
            last_price = None
            while not SHUTDOWN:
                try:
                    quote = get_latest_quote(TICKER)
                except Exception as e:
                    print(f"Error fetching quote: {e}")
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                # DEBUG: log every fetched price, even ones filtered out by dedupe below,
                # so we can tell whether yfinance is actually returning fresh values.
                print(f"Fetched: market_time={quote['date']} -> {quote['current']} (polled at {quote['fetched_at']})")

                # Only publish when the price actually changes, to avoid flooding duplicate ticks
                if quote["current"] != last_price:
                    msgSeqNum += 1
                    # Check https://docs.solace.com/API-Developer-Online-Ref-Documentation/python/source/rst/solace.messaging.config.solace_properties.html for additional message properties
                    # Note: additional properties override what is set by the message_builder
                    additional_properties = {APPLICATION_MESSAGE_ID: f'sample_id {msgSeqNum}'}
                    payload = json.dumps(quote)
                    outbound_message = message_builder.build(payload, additional_message_properties=additional_properties)
                    # Direct publish the message
                    direct_publisher.publish(destination=Topic.of(TOPIC_PREFIX + f"/python/stocks/{TICKER}/{msgSeqNum}"), message=outbound_message)
                    print(f"Published: {quote['date']} -> {quote['current']}")
                    last_price = quote["current"]

                time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print('\nDisconnecting Messaging Service')
        except PubSubPlusClientError as exception:
            print(f'Received a PubSubPlusClientException: {exception}')
    finally:
        print('Terminating Publisher and Receiver')
        direct_publisher.terminate()
        direct_receiver.terminate()
        print('Disconnecting Messaging Service')
        messaging_service.disconnect()

if __name__ == "__main__":
    run_streamer()
