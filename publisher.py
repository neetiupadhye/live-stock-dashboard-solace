"""
publisher.py

Standalone publisher: polls yfinance for the latest price of TICKER
and publishes each new tick to the Solace broker.

Runs completely independently of the subscriber/dashboard — you can
run this on a different machine than the one running subscriber.py +
dashboard.py, as long as both point at the same Solace broker (see
solace_common.py and the SOLACE_* environment variables).

Run directly:

    python3 publisher.py
"""

import time
import json

from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener, FailedPublishEvent
from solace.messaging.config.solace_properties.message_properties import APPLICATION_MESSAGE_ID
from solace.messaging.resources.topic import Topic

import yfinance as yf

from solace_common import TOPIC_PREFIX, build_messaging_service, attach_service_listeners

TICKER = "D05.SI"   # DBS Group Holdings, listed on SGX — swap back to "AAPL" for US market hours testing
POLL_INTERVAL_SECONDS = 15   # 1-minute bars can't update faster than once a minute anyway; 15s just catches the new bar promptly


def get_latest_quote(ticker_symbol):
    """
    Fetch the most recent price tick for a ticker via yfinance.

    Uses history(period="1d", interval="1m") rather than Ticker.fast_info,
    since fast_info hits a lightweight quote endpoint that can return a
    stale cached price if polled faster than its own refresh interval.
    """
    ticker_obj = yf.Ticker(ticker_symbol)
    bars = ticker_obj.history(period="1d", interval="1m")
    if bars.empty:
        raise RuntimeError(f"No intraday data returned for {ticker_symbol} (market may be closed)")

    last_bar = bars.iloc[-1]
    bar_timestamp = bars.index[-1]   # the actual market timestamp this price applies to

    return {
        "date": bar_timestamp.isoformat(),
        "ticker": ticker_symbol,
        "current": float(last_bar["Close"]),
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S"),   # debug only: wall-clock poll time
    }


def get_intraday_history(ticker_symbol):
    """
    Fetch every 1-minute bar for the current trading day so far, oldest
    first. Used once at startup to backfill the chart from market open
    up to now, before switching over to live polling of just the latest
    bar via get_latest_quote().
    """
    ticker_obj = yf.Ticker(ticker_symbol)
    bars = ticker_obj.history(period="1d", interval="1m")
    if bars.empty:
        return []

    fetched_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    quotes = []
    for bar_timestamp, row in bars.iterrows():
        quotes.append({
            "date": bar_timestamp.isoformat(),
            "ticker": ticker_symbol,
            "current": float(row["Close"]),
            "fetched_at": fetched_at,
        })
    return quotes


class PublisherErrorHandling(PublishFailureListener):
    def on_failed_publish(self, e: "FailedPublishEvent"):
        print("on_failed_publish")


def run_publisher():
    messaging_service = build_messaging_service()
    attach_service_listeners(messaging_service)

    direct_publisher = messaging_service.create_direct_message_publisher_builder().build()
    direct_publisher.set_publish_failure_listener(PublisherErrorHandling())
    direct_publisher.start()

    message_builder = (
        messaging_service.message_builder()
        .with_application_message_id("sample_id")
        .with_property("application", "samples")
        .with_property("language", "Python")
    )

    msg_seq_num = 0

    def publish_quote(quote):
        nonlocal msg_seq_num
        msg_seq_num += 1
        additional_properties = {APPLICATION_MESSAGE_ID: f"sample_id {msg_seq_num}"}
        payload = json.dumps(quote)
        outbound_message = message_builder.build(payload, additional_message_properties=additional_properties)
        direct_publisher.publish(
            destination=Topic.of(TOPIC_PREFIX + f"/python/stocks/{TICKER}/{msg_seq_num}"),
            message=outbound_message,
        )
        print(f"Published: {quote['date']} -> {quote['current']}")

    try:
        print(f"Publishing {TICKER} to {TOPIC_PREFIX}/python/stocks/{TICKER}/ every {POLL_INTERVAL_SECONDS}s...\n")

        # --- One-time backfill: send every 1-minute bar from market
        # open up to now, so a dashboard opened mid-session (or before
        # any live ticks have happened) still shows the full day so far,
        # instead of only starting to plot from whenever it connected.
        last_published_date = None
        try:
            history = get_intraday_history(TICKER)
        except Exception as e:
            print(f"Error fetching backfill history: {e}")
            history = []

        if history:
            print(f"Backfilling {len(history)} bars from today's session...")
            for quote in history:
                publish_quote(quote)
                last_published_date = quote["date"]
            print("Backfill complete. Switching to live polling.\n")

        # --- Live polling: from here on, just publish the latest bar
        # every POLL_INTERVAL_SECONDS, same as before.
        while True:
            try:
                quote = get_latest_quote(TICKER)
            except Exception as e:
                print(f"Error fetching quote: {e}")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            print(f"Fetched: market_time={quote['date']} -> {quote['current']} (polled at {quote['fetched_at']})")

            # Skip re-publishing the same 1-minute bar the backfill (or
            # the previous poll) already sent — a new bar only exists
            # once yfinance rolls over to the next minute.
            if quote["date"] == last_published_date:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            publish_quote(quote)
            last_published_date = quote["date"]

            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nDisconnecting Messaging Service")
    except PubSubPlusClientError as exception:
        print(f"Received a PubSubPlusClientException: {exception}")
    finally:
        print("Terminating Publisher")
        direct_publisher.terminate()
        print("Disconnecting Messaging Service")
        messaging_service.disconnect()


if __name__ == "__main__":
    run_publisher()
