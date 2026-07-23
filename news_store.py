"""
news_store.py

A small, thread-safe in-memory store for the latest news articles per
ticker — the news equivalent of data_store.py.

Why this exists, and why it's separate from data_store.py:
- Different message shape (a list of articles vs. a price point) and
  a much lower update frequency (every few minutes vs. every second),
  so it doesn't belong in the same deque-based per-tick structure.
- Same producer/consumer split as price data though: subscriber.py
  writes into this on its own thread as news messages arrive over
  Solace; dashboard.py reads from it on a timer on the main thread.
  Neither side ever calls yfinance directly — only publisher.py does,
  keeping the dashboard fully decoupled from the data source.
"""

import threading


class NewsStore:
    def __init__(self):
        # ticker -> {"articles": [...], "fetched_at": "..."}
        self._news = {}
        self.lock = threading.Lock()

    def set(self, ticker, articles, fetched_at=None):
        """Replace the stored articles for `ticker`. Safe to call from
        any thread. Each publish carries a fresh snapshot rather than
        an incremental update, so this is a plain replace, not an
        append."""
        with self.lock:
            self._news[ticker] = {"articles": articles, "fetched_at": fetched_at}

    def get(self, ticker):
        """Return a snapshot copy of the current article list for
        `ticker`. Empty list if nothing has been received for that
        ticker yet. Safe to call from any thread."""
        with self.lock:
            entry = self._news.get(ticker)
            return list(entry["articles"]) if entry else []

    def get_fetched_at(self, ticker):
        """When the currently-stored articles for `ticker` were fetched
        by the publisher, or None if nothing has been received yet."""
        with self.lock:
            entry = self._news.get(ticker)
            return entry["fetched_at"] if entry else None

    def clear(self, ticker=None):
        """Wipe stored news. Pass a ticker to clear just that one, or
        omit it to wipe everything."""
        with self.lock:
            if ticker is None:
                self._news.clear()
            else:
                self._news.pop(ticker, None)


# A single shared instance — both subscriber.py and dashboard.py
# import THIS object, same convention as data_store.
news_store = NewsStore()
