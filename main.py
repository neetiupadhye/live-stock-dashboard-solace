"""
main.py

The single entry point for the whole project. Run this file — not the
others directly — to start everything together:

    python3 main.py

What it does:
1. Starts stock_price_streamer.run_streamer() on a background thread.
   This connects to Solace, publishes rows from the stock dataset,
   receives them back, and writes each data point into data_store.
2. Starts dashboard.run_dashboard() on the main thread. This runs the
   Dash web server, which reads from the SAME data_store on a timer
   and redraws the live chart.

Both sides share one process, so they share one data_store instance
in memory — see data_store.py for why that matters.
"""

import threading

from stock_price_streamer import run_streamer
from dashboard import run_dashboard


def start_streamer_thread():
    # daemon=True means this thread is automatically killed when the
    # main program exits (e.g. Ctrl+C on the dashboard) — otherwise it
    # would keep the process alive in the background forever.
    thread = threading.Thread(target=run_streamer, daemon=True)
    thread.start()
    return thread


if __name__ == "__main__":
    print("Starting Solace streamer on a background thread...")
    start_streamer_thread()

    print("Starting dashboard at http://127.0.0.1:8050 ...")
    # This call blocks — it runs the Dash server until you stop the
    # program. Nothing after this line will execute.
    run_dashboard()
