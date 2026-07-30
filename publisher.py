"""
publisher.py

Standalone publisher: polls yfinance for the latest price of whichever
tickers are currently being watched by a dashboard, and publishes each
new tick to the Solace broker.

Runs completely independently of the subscriber/dashboard — you can
run this on a different machine than the one running subscriber.py +
dashboard.py, as long as both point at the same Solace broker (see
solace_common.py and the SOLACE_* environment variables).

Run directly:

    python3 publisher.py

--- Why this doesn't just loop over every SGX ticker ---
solace_common.ALL_SGX_TICKERS/AVAILABLE_TICKERS is the *dropdown*
universe (all ~500 SGX-listed codes) — it is deliberately NOT what
this file polls. Polling 500 tickers every cycle would mean each
cycle takes far longer than POLL_INTERVAL_SECONDS to even get through
everyone once, and hammering yfinance with hundreds of sequential
requests on a tight loop is a good way to get rate-limited.

Instead, this file maintains an ACTIVE-TICKER SET: only tickers a
dashboard is actually watching right now. A ticker enters the active
set when a "start" backfill-control message arrives (some dashboard
just switched to it, or started up on it) and leaves it when a "stop"
message arrives (the dashboard switched away), with an idle timeout as
a safety net in case a stop message is ever lost (e.g. the subscriber
process crashes mid-session).
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
    topic_for_news,
    BACKFILL_REQUEST_TOPIC_PREFIX,
    build_messaging_service,
    attach_service_listeners,
)

POLL_INTERVAL_SECONDS = 15   # 1-minute bars can't update faster than once a minute anyway; 15s just catches the new bar promptly
# NOTE: this interval applies per poll cycle, and each cycle polls
# every ticker currently in the ACTIVE set (see _active_tickers
# below) — not the full SGX universe. The active set is normally
# small (one ticker per open dashboard), so this stays fast regardless
# of how big AVAILABLE_TICKERS/ALL_SGX_TICKERS grows.

# News doesn't need per-tick freshness the way prices do, so it's
# polled on its own, much slower cadence, tracked per-ticker below.
NEWS_POLL_INTERVAL_SECONDS = 300   # 5 minutes

# How many articles to keep per ticker per publish — keeps messages
# small and the sidebar box from growing unbounded.
NEWS_ARTICLES_PER_TICKER = 5

# Safety-net eviction: if a ticker's "stop" message is ever lost
# (subscriber crash, network blip), this is the fallback that keeps
# the active set from only ever growing. Deliberately long, since the
# *normal* path for leaving the active set is the explicit stop
# message sent the moment a dashboard switches away — this is just
# cleanup for orphans, not the everyday mechanism, so it's set well
# above any realistic "still looking at the same stock" session.
IDLE_EVICTION_SECONDS = 4 * 60 * 60  # 4 hours

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


def get_latest_news(ticker_symbol):
    """
    Fetch the most recent news articles for a ticker via yfinance,
    normalized down to just the fields the dashboard actually shows.

    yfinance's .news shape has changed across versions (article fields
    sometimes live directly on the item, sometimes nested under a
    "content" key) — this normalizes both so the rest of the pipeline
    doesn't need to care which one a given yfinance version returns.
    """
    ticker_obj = yf.Ticker(ticker_symbol)
    raw_items = ticker_obj.news or []

    articles = []
    for item in raw_items[:NEWS_ARTICLES_PER_TICKER]:
        content = item.get("content", item)  # newer yfinance nests fields under "content"

        title = content.get("title")
        if not title:
            continue  # skip anything we can't even show a headline for

        link = (
            content.get("clickThroughUrl", {}).get("url")
            or content.get("canonicalUrl", {}).get("url")
            or item.get("link")
            or ""
        )
        publisher_name = (
            content.get("provider", {}).get("displayName")
            or item.get("publisher")
            or ""
        )
        published = content.get("pubDate") or content.get("displayTime") or ""

        articles.append({
            "title": title,
            "link": link,
            "publisher": publisher_name,
            "published": published,
        })

    return articles


# ---------------------------------------------------------------------
# Active-ticker set
#
# ticker -> {
#   "added_at": monotonic time this ticker entered the active set,
#   "last_published_date": the "date" field of the last quote we
#       published for this ticker, so the poll loop doesn't republish
#       the same 1-minute bar every cycle,
#   "last_news_at": monotonic time news was last fetched+published for
#       this ticker, or None if never (so the very first poll cycle
#       after activation always refreshes news, matching the old
#       "fetch news immediately on backfill" behavior),
# }
#
# Guarded by _active_lock since BackfillControlHandler runs on the
# SDK's message-callback thread while the main polling loop runs on
# the main thread.
# ---------------------------------------------------------------------
_active_tickers = {}
_active_lock = threading.Lock()


def _activate_ticker(ticker):
    """Add `ticker` to the active set (or just leave it as-is if
    already active — a repeat "start" shouldn't reset its
    last_published_date/last_news_at and cause a duplicate replay)."""
    with _active_lock:
        if ticker not in _active_tickers:
            _active_tickers[ticker] = {
                "added_at": time.monotonic(),
                "last_published_date": None,
                "last_news_at": None,
            }
            return True
        return False


def _deactivate_ticker(ticker):
    with _active_lock:
        _active_tickers.pop(ticker, None)


def _active_snapshot():
    """The list of currently-active tickers, safe to iterate over
    without holding the lock for the whole poll cycle."""
    with _active_lock:
        return list(_active_tickers.keys())


def _get_meta(ticker):
    with _active_lock:
        return _active_tickers.get(ticker)


def _set_last_published_date(ticker, date):
    with _active_lock:
        if ticker in _active_tickers:
            _active_tickers[ticker]["last_published_date"] = date


def _set_last_news_at(ticker, when):
    with _active_lock:
        if ticker in _active_tickers:
            _active_tickers[ticker]["last_news_at"] = when


def _evict_idle_tickers():
    """Safety-net cleanup — see IDLE_EVICTION_SECONDS above. The
    normal path out of the active set is an explicit "stop" message,
    not this."""
    now = time.monotonic()
    with _active_lock:
        stale = [t for t, meta in _active_tickers.items() if now - meta["added_at"] > IDLE_EVICTION_SECONDS]
        for t in stale:
            del _active_tickers[t]
    for t in stale:
        print(f"Evicting {t} from active polling (idle timeout, no stop message received)")


class PublisherErrorHandling(PublishFailureListener):
    def on_failed_publish(self, e: "FailedPublishEvent"):
        print("on_failed_publish")


class BackfillControlHandler(MessageHandler):
    """
    Listens on BACKFILL_REQUEST_TOPIC_PREFIX/{ticker}. Each message
    carries {"ticker": ..., "action": "start"|"stop"}:

    - "start": fired when a dashboard starts up on a ticker or
      switches to one. Activates the ticker for ongoing live polling
      (if not already active) and immediately refetches its
      day-so-far bars plus fresh news — the same "full history, then
      live" treatment every activated ticker gets.
    - "stop": fired when a dashboard switches AWAY from a ticker.
      Removes it from the active set so the publisher stops spending
      poll cycles on stocks nobody is currently watching.

    A single Direct Receiver only takes one MessageHandler, so both
    directions share this one handler rather than two separate ones.
    """

    def __init__(self, publish_quote_fn, publish_news_fn):
        self._publish_quote = publish_quote_fn
        self._publish_news = publish_news_fn

    def on_message(self, message: "InboundMessage"):
        try:
            payload = message.get_payload_as_string() or message.get_payload_as_bytes()
            if isinstance(payload, bytearray):
                payload = payload.decode()
            data = json.loads(payload)
            ticker = data.get("ticker")
            action = data.get("action", "start")  # default "start" keeps old callers (no action field) working
        except Exception as e:
            print(f"Error processing backfill control message: {e}")
            return

        if not ticker or ticker not in AVAILABLE_TICKERS:
            return

        if action == "stop":
            _deactivate_ticker(ticker)
            print(f"Stop received for {ticker}, no longer actively polling it")
            return

        is_new = _activate_ticker(ticker)
        if not is_new:
            # Already active (e.g. a duplicate/retried "start") — no
            # need to replay history again.
            return

        # Run the actual yfinance fetch + republish on its own thread
        # so a slow fetch doesn't block the SDK's message-callback
        # thread (which would delay processing of other requests).
        threading.Thread(target=self._replay, args=(ticker,), daemon=True).start()

    def _replay(self, ticker):
        print(f"Activating {ticker}: fetching day-so-far history...")
        try:
            history = get_intraday_history(ticker)
        except Exception as e:
            print(f"Error fetching backfill history for {ticker}: {e}")
            history = []

        for quote in history:
            self._publish_quote(quote)
        if history:
            _set_last_published_date(ticker, history[-1]["date"])
        print(f"Backfill replay complete for {ticker} ({len(history)} bars)")

        try:
            self._publish_news(ticker)
            _set_last_news_at(ticker, time.monotonic())
        except Exception as e:
            print(f"Error fetching initial news for {ticker}: {e}")


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

    def publish_news(ticker):
        nonlocal msg_seq_num
        articles = get_latest_news(ticker)
        msg_seq_num += 1
        additional_properties = {APPLICATION_MESSAGE_ID: f"sample_id {msg_seq_num}"}
        payload = json.dumps({
            "ticker": ticker,
            "articles": articles,
            "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        })
        outbound_message = message_builder.build(payload, additional_message_properties=additional_properties)
        direct_publisher.publish(
            destination=Topic.of(topic_for_news(ticker)),
            message=outbound_message,
        )
        print(f"Published [{ticker}] news: {len(articles)} article(s)")

    # Listens for on-demand start/stop control messages (see
    # BackfillControlHandler above) so the active-ticker set stays
    # sized to whatever dashboards are actually watching, instead of
    # this file ever needing to loop over the full SGX universe.
    backfill_receiver = (
        messaging_service.create_direct_message_receiver_builder()
        .with_subscriptions([TopicSubscription.of(BACKFILL_REQUEST_TOPIC_PREFIX + "/>")])
        .build()
    )
    backfill_receiver.start()
    backfill_receiver.receive_async(BackfillControlHandler(publish_quote, publish_news))

    try:
        print(f"Publisher ready. Serving on-demand backfill for any of {len(AVAILABLE_TICKERS)} SGX tickers.")
        print(f"Actively polling only tickers currently watched by a dashboard, every {POLL_INTERVAL_SECONDS}s.\n")

        last_eviction_check = time.monotonic()
        eviction_check_interval = 60  # only need to check idle eviction occasionally, not every poll cycle

        # --- Live polling: each cycle, poll only the currently-active
        # tickers (dashboards actually watching something), and check
        # each active ticker's own news timer independently.
        while True:
            for ticker in _active_snapshot():
                meta = _get_meta(ticker)
                if meta is None:
                    continue  # deactivated between the snapshot and now

                try:
                    quote = get_latest_quote(ticker)
                except Exception as e:
                    print(f"Error fetching quote for {ticker}: {e}")
                    continue

                print(f"Fetched [{ticker}] ({'LIVE' if quote['is_live'] else 'CLOSED'}): market_time={quote['date']} -> {quote['current']} (polled at {quote['fetched_at']})")

                if quote["date"] != meta["last_published_date"]:
                    publish_quote(quote)
                    _set_last_published_date(ticker, quote["date"])

                # Per-ticker news timer: each ticker checks its own
                # cadence rather than everyone sharing one global
                # timer, since tickers get activated at different
                # times (naturally staggering their news refreshes).
                last_news_at = meta["last_news_at"]
                if last_news_at is None or time.monotonic() - last_news_at >= NEWS_POLL_INTERVAL_SECONDS:
                    try:
                        publish_news(ticker)
                        _set_last_news_at(ticker, time.monotonic())
                    except Exception as e:
                        print(f"Error fetching news for {ticker}: {e}")

            if time.monotonic() - last_eviction_check >= eviction_check_interval:
                _evict_idle_tickers()
                last_eviction_check = time.monotonic()

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
