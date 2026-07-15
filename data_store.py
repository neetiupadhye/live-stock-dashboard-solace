"""
data_store.py

A small, thread-safe in-memory store for streaming stock data.

Why this exists:
- The Solace receiver (stock_price_streamer.py) writes new data points
  as messages arrive, on its own background thread.
- The Dash dashboard (dashboard.py) reads the data on a timer, on the
  main thread.
- Both need to safely touch the same data at the same time without
  corrupting it — this class handles that with a lock.
"""

import threading
from collections import deque


class DataStore:
    def __init__(self, max_points=500):
        """
        max_points: how many recent data points to keep in memory.
        Older points are automatically dropped once this limit is
        reached (keeps memory bounded for a long-running stream).
        """
        self.dates = deque(maxlen=max_points)
        self.currents = deque(maxlen=max_points)
        self.lock = threading.Lock()

    def add(self, date, current_price):
        """Add one new data point. Safe to call from any thread."""
        with self.lock:
            self.dates.append(date)
            self.currents.append(current_price)

    def get_data(self):
        """
        Return a snapshot copy of the current data as two plain lists:
        (dates, currents). Safe to call from any thread.
        """
        with self.lock:
            return list(self.dates), list(self.currents)

    def clear(self):
        """Wipe all stored data."""
        with self.lock:
            self.dates.clear()
            self.currents.clear()


# A single shared instance — both stock_price_streamer.py and
# dashboard.py will import THIS object, so they're both reading from
# and writing to the same data.
data_store = DataStore()


# Quick manual test — run this file directly to sanity-check it works
# before wiring it into the streamer or dashboard.
if __name__ == "__main__":
    data_store.add("2024-01-01", 100.5)
    data_store.add("2024-01-02", 101.2)
    data_store.add("2024-01-03", 99.8)

    dates, currents = data_store.get_data()
    print("Dates:", dates)
    print("Currents:", currents)
