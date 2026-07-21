"""
data_store.py

A small, thread-safe in-memory store for streaming stock data — now
keyed per ticker, so it can hold history for multiple stocks at once
(the dashboard only ever displays one at a time, but keeping each
ticker's series around means switching back to a stock you looked at
earlier doesn't lose what was already collected for it).

Why this exists:
- The Solace receiver (subscriber.py) writes new data points as
  messages arrive, on its own background thread. Each message's
  payload carries its own "ticker" field, so the store doesn't need
  to know or care which topic the subscriber is currently subscribed
  to — it just files each point under the ticker named in the message.
- The Dash dashboard (dashboard.py) reads the data for whichever
  ticker is currently selected, on a timer, on the main thread.
- Both need to safely touch the same data at the same time without
  corrupting it — this class handles that with a lock.
"""

import threading
from collections import deque


class DataStore:
    def __init__(self, max_points=500):
        """
        max_points: how many recent data points to keep in memory,
        PER TICKER. Older points are automatically dropped once this
        limit is reached (keeps memory bounded for a long-running
        stream, even with several tickers accumulating history).
        """
        self._max_points = max_points
        # ticker -> {"dates": deque, "currents": deque}, created
        # lazily the first time a point for that ticker arrives.
        self._series = {}
        self.lock = threading.Lock()

    def add(self, ticker, date, current_price, is_live=False):
        """Add one new data point for `ticker`, tagged with whether the
        market it came from is currently live/open. Safe to call from
        any thread."""
        with self.lock:
            series = self._series.get(ticker)
            if series is None:
                series = {
                    "dates": deque(maxlen=self._max_points),
                    "currents": deque(maxlen=self._max_points),
                    "is_live": False,
                }
                self._series[ticker] = series

            # A backfill reply and an in-flight live poll can, in rare
            # cases, both deliver the same bar (same date) around a
            # ticker switch. Update it in place rather than appending
            # a duplicate point for the same timestamp.
            if series["dates"] and series["dates"][-1] == date:
                series["currents"][-1] = current_price
                series["is_live"] = is_live
                return

            series["dates"].append(date)
            series["currents"].append(current_price)
            series["is_live"] = is_live

    def get_is_live(self, ticker):
        """Whether the most recent data point stored for `ticker` came
        from a live/open market, vs. a closed session's last price.
        Returns False if nothing has been received for this ticker yet."""
        with self.lock:
            series = self._series.get(ticker)
            return series["is_live"] if series else False

    def get_data(self, ticker):
        """
        Return a snapshot copy of the current data for `ticker` as two
        plain lists: (dates, currents). Empty lists if nothing has
        been received for that ticker yet. Safe to call from any thread.
        """
        with self.lock:
            series = self._series.get(ticker)
            if series is None:
                return [], []
            return list(series["dates"]), list(series["currents"])

    def clear(self, ticker=None):
        """Wipe stored data. Pass a ticker to clear just that one, or
        omit it to wipe everything."""
        with self.lock:
            if ticker is None:
                self._series.clear()
            else:
                self._series.pop(ticker, None)


# A single shared instance — both subscriber.py and dashboard.py will
# import THIS object, so they're both reading from and writing to the
# same data.
data_store = DataStore()


# Quick manual test — run this file directly to sanity-check it works
# before wiring it into the subscriber or dashboard.
if __name__ == "__main__":
    data_store.add("D05.SI", "2024-01-01", 100.5)
    data_store.add("D05.SI", "2024-01-02", 101.2)
    data_store.add("AAPL", "2024-01-01", 190.0)

    dates, currents = data_store.get_data("D05.SI")
    print("D05.SI dates:", dates)
    print("D05.SI currents:", currents)

    dates, currents = data_store.get_data("AAPL")
    print("AAPL dates:", dates)
    print("AAPL currents:", currents)
