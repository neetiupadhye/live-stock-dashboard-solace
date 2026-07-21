"""
subscriber.py

Standalone subscriber: connects to the Solace broker, subscribes to
the stock ticks topic, and writes every received data point into
data_store.

This is what feeds the dashboard, so it needs to run in the same
process as dashboard.py (see main.py) since they share the in-memory
data_store. It does NOT need to run on the same machine as the
publisher — only the same Solace broker.

Run directly (useful for testing the subscriber on its own, without
the dashboard):

    python3 subscriber.py
"""

import json
import time

from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.receiver.inbound_message import InboundMessage

from data_store import data_store
from solace_common import TOPIC_PREFIX, build_messaging_service, attach_service_listeners


class MessageHandlerImpl(MessageHandler):
    def on_message(self, message: "InboundMessage"):
        try:
            payload = message.get_payload_as_string() or message.get_payload_as_bytes()
            if isinstance(payload, bytearray):
                payload = payload.decode()

            data = json.loads(payload)
            data_store.add(data["date"], data["current"])

            print(f"Stored: {data['date']} -> {data['current']}")

        except Exception as e:
            print(f"Error processing message: {e}")


def run_subscriber():
    messaging_service = build_messaging_service()
    attach_service_listeners(messaging_service)

    topics = [TOPIC_PREFIX + "/python/stocks/>"]
    topics_sub = [TopicSubscription.of(t) for t in topics]

    direct_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(topics_sub).build()

    try:
        direct_receiver.start()
        direct_receiver.receive_async(MessageHandlerImpl())
        if direct_receiver.is_running():
            print(f"Subscribed to: {topics}\nReady to receive\n")

        # The Solace API delivers messages on its own callback thread,
        # so this loop just needs to keep the process/thread alive
        # until it's told to stop.
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDisconnecting Messaging Service")
    except PubSubPlusClientError as exception:
        print(f"Received a PubSubPlusClientException: {exception}")
    finally:
        print("Terminating Receiver")
        direct_receiver.terminate()
        print("Disconnecting Messaging Service")
        messaging_service.disconnect()


if __name__ == "__main__":
    run_subscriber()
