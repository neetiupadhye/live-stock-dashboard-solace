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
import threading
import datetime

from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener, FailedPublishEvent
from solace.messaging.config.solace_properties.message_properties import APPLICATION_MESSAGE_ID
from solace.messaging.resources.topic import Topic
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.receiver.inbound_message import InboundMessage

import yfinance as yf

from solace_common import (
    AVAILABLE_TICKERS,
    topic_for_ticker,
    BACKFILL_REQUEST_TOPIC_PREFIX,
    build_messaging_service,
    attach_service_listeners,
)

POLL_INTERVAL_SECONDS = 15   # 1-minute bars can't update faster than once a minute anyway; 15s just catches the new bar promptly
# NOTE: this interval applies per poll cycle, and each cycle now polls
# every ticker in AVAILABLE_TICKERS in turn. If you add a lot of
# tickers and start seeing yfinance rate-limit errors, raise this.

# How recent a bar's own timestamp has to be, relative to wall-clock
# now, to count as "live" rather than "last known price from a closed
# session". 1-minute bars only update while a market is actually
# trading, so once a market closes, yfinance just keeps returning the
# same final bar forever — this is what lets us tell the two apart.
LIVE_FRESHNESS_WINDOW_SECONDS = 180


def _is_bar_live(bar_timestamp):
    """Whether `bar_timestamp` (a tz-aware pandas Timestamp) is recent
    enough that we consider its market open/live right now."""
    now = datetime.datetime.now(bar_timestamp.tzinfo)
    return (now - bar_timestamp) <= datetime.timedelta(seconds=LIVE_FRESHNESS_WINDOW_SECONDS)


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
        "is_live": _is_bar_live(bar_timestamp),
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
            "is_live": _is_bar_live(bar_timestamp),
        })
    return quotes


class PublisherErrorHandling(PublishFailureListener):
    def on_failed_publish(self, e: "FailedPublishEvent"):
        print("on_failed_publish")


class BackfillRequestHandler(MessageHandler):
    """
    Listens on BACKFILL_REQUEST_TOPIC_PREFIX/{ticker}. Whenever the
    subscriber switches to a ticker it doesn't have history for yet,
    it publishes a request here; this handler refetches that ticker's
    day-so-far bars and republishes them on the normal data topic —
    the same get_intraday_history() + publish_quote() path used for
    the one-time startup backfill, just re-triggered on demand.
    """

    def __init__(self, publish_quote_fn):
        self._publish_quote = publish_quote_fn

    def on_message(self, message: "InboundMessage"):
        try:
            payload = message.get_payload_as_string() or message.get_payload_as_bytes()
            if isinstance(payload, bytearray):
                payload = payload.decode()
            data = json.loads(payload)
            ticker = data.get("ticker")
        except Exception as e:
            print(f"Error processing backfill request: {e}")
            return

        if not ticker or ticker not in AVAILABLE_TICKERS:
            return

        # Run the actual yfinance fetch + republish on its own thread
        # so a slow fetch doesn't block the SDK's message-callback
        # thread (which would delay processing of other requests/ticks).
        threading.Thread(target=self._replay, args=(ticker,), daemon=True).start()

    def _replay(self, ticker):
        print(f"Backfill requested for {ticker}, refetching day-so-far history...")
        try:
            history = get_intraday_history(ticker)
        except Exception as e:
            print(f"Error fetching backfill history for {ticker}: {e}")
            return

        for quote in history:
            self._publish_quote(quote)
        print(f"Backfill replay complete for {ticker} ({len(history)} bars)")


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
            destination=Topic.of(topic_for_ticker(quote["ticker"])),
            message=outbound_message,
        )
        print(f"Published [{quote['ticker']}]: {quote['date']} -> {quote['current']}")

    # Listens for on-demand backfill requests (see BackfillRequestHandler)
    # so a dashboard switching tickers can get the same "full day-so-far
    # then live" treatment the very first ticker gets at startup.
    backfill_receiver = (
        messaging_service.create_direct_message_receiver_builder()
        .with_subscriptions([TopicSubscription.of(BACKFILL_REQUEST_TOPIC_PREFIX + "/>")])
        .build()
    )
    backfill_receiver.start()
    backfill_receiver.receive_async(BackfillRequestHandler(publish_quote))

    try:
        print(f"Publishing {AVAILABLE_TICKERS} (one topic per ticker) every {POLL_INTERVAL_SECONDS}s...\n")

        # Tracks the last bar timestamp we published per ticker, so we
        # don't republish the same 1-minute bar every cycle.
        last_published_date = {ticker: None for ticker in AVAILABLE_TICKERS}

        # --- One-time backfill: for each ticker, send every 1-minute
        # bar from market open up to now, so a dashboard opened
        # mid-session (or before any live ticks have happened) still
        # shows the full day so far for whichever stock it picks.
        for ticker in AVAILABLE_TICKERS:
            try:
                history = get_intraday_history(ticker)
            except Exception as e:
                print(f"Error fetching backfill history for {ticker}: {e}")
                history = []

            if history:
                print(f"Backfilling {len(history)} bars for {ticker}...")
                for quote in history:
                    publish_quote(quote)
                    last_published_date[ticker] = quote["date"]
        print("Backfill complete. Switching to live polling.\n")

        # --- Live polling: each cycle, poll every ticker in turn and
        # publish only the ones with a new bar since last time.
        while True:
            for ticker in AVAILABLE_TICKERS:
                try:
                    quote = get_latest_quote(ticker)
                except Exception as e:
                    print(f"Error fetching quote for {ticker}: {e}")
                    continue

                print(f"Fetched [{ticker}] ({'LIVE' if quote['is_live'] else 'CLOSED'}): market_time={quote['date']} -> {quote['current']} (polled at {quote['fetched_at']})")

                # Skip re-publishing the same 1-minute bar the backfill
                # (or the previous poll) already sent for this ticker.
                if quote["date"] == last_published_date[ticker]:
                    continue

                publish_quote(quote)
                last_published_date[ticker] = quote["date"]

            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nDisconnecting Messaging Service")
    except PubSubPlusClientError as exception:
        print(f"Received a PubSubPlusClientException: {exception}")
    finally:
        print("Terminating Publisher")
        direct_publisher.terminate()
        backfill_receiver.terminate()
        print("Disconnecting Messaging Service")
        messaging_service.disconnect()


if __name__ == "__main__":
    run_publisher()
