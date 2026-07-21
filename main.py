"""
main.py

Entry point for the RECEIVING side of the project: the subscriber and
the dashboard. Run this file to start both together:

    python3 main.py

What it does:
1. Starts subscriber.run_subscriber() on a background thread. This
   connects to Solace, subscribes to the stock ticks topic, and
   writes each received data point into data_store.
2. Starts dashboard.run_dashboard() on the main thread. This runs the
   Dash web server, which reads from the SAME data_store on a timer
   and redraws the live chart.

Both sides share one process, so they share one data_store instance
in memory — see data_store.py for why that matters.

NOTE: the publisher is a separate, standalone script now — see
publisher.py. Run it independently, on this machine or any other,
as long as it points (via the SOLACE_* env vars) at the same broker
this subscriber is connected to:

    python3 publisher.py
"""

import threading

from subscriber import run_subscriber
from dashboard import run_dashboard
from solace_common import AVAILABLE_TICKERS


def start_subscriber_thread():
    # daemon=True means this thread is automatically killed when the
    # main program exits (e.g. Ctrl+C on the dashboard) — otherwise it
    # would keep the process alive in the background forever.
    # Starts subscribed to the first ticker in AVAILABLE_TICKERS,
    # matching the dashboard's default dropdown selection; the
    # dashboard re-points the subscription via switch_ticker()
    # whenever the user picks a different stock.
    thread = threading.Thread(
        target=run_subscriber, kwargs={"initial_ticker": AVAILABLE_TICKERS[0]}, daemon=True
    )
    thread.start()
    return thread


if __name__ == "__main__":
    print("Starting Solace subscriber on a background thread...")
    start_subscriber_thread()

    print("Starting dashboard at http://127.0.0.1:8050 ...")
    print("(Remember: run publisher.py separately to actually feed it data)")
    # This call blocks — it runs the Dash server until you stop the
    # program. Nothing after this line will execute.
    run_dashboard()
