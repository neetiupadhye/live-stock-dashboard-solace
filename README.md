# Live Stock Price Streamer & Dashboard

A real-time stock price pipeline built on **Solace PubSub+** event streaming, with live market data pulled from **Yahoo Finance** and visualized in an auto-refreshing **Dash** web dashboard.

The project demonstrates an end-to-end event-driven architecture: a background thread polls live market data, publishes it onto a Solace message broker, a receiver consumes it back off the broker, and a thread-safe in-memory store feeds a live-updating chart on the main thread.

---

## Architecture

```
┌─────────────────┐      ┌───────────────────┐      ┌──────────────────┐
│  Yahoo Finance   │      │   Solace PubSub+   │      │   Dash Web App   │
│  (yfinance API)  │      │   Event Broker      │      │  (localhost:8050)│
└────────┬─────────┘      └─────────┬──────────┘      └────────┬─────────┘
         │ poll every 15s           │                          │
         ▼                          │                          │
┌──────────────────┐   publish      │      receive             │
│ stock_price_      ├───────────────►                          │
│ streamer.py       │◄──────────────┤                          │
│ (background       │  subscribe    │                          │
│  thread)           │                                          │
└────────┬───────────┘                                          │
         │ write                                                │
         ▼                                                      │
┌──────────────────┐   read (1x/sec)                            │
│   data_store.py   ├──────────────────────────────────────────►│
│ (thread-safe       │                                          │
│  shared store)     │                                          │
└────────────────────┘                                dashboard.py
```

`main.py` wires everything together: it starts the streamer on a daemon background thread and runs the Dash server on the main thread. Both sides share a single `data_store` instance in memory, guarded by a lock, so producer and consumer never race.

---

## How It Works

1. **`stock_price_streamer.py`** polls Yahoo Finance every 15 seconds for the latest 1-minute price bar of a configured ticker.
2. If the price has changed since the last poll, it publishes the quote as a JSON message to a Solace topic (`solace/samples/python/stocks/<TICKER>/<seq>`).
3. The same process also subscribes to that topic hierarchy, receives the message back, and writes the data point into the shared `data_store`.
4. **`dashboard.py`** runs a Dash app that polls `data_store` once per second and redraws a live line chart of price vs. market time.
5. **`main.py`** is the single entry point — it starts the streamer thread and then blocks on the Dash server.

This publish → receive → store round-trip (rather than writing directly from the poller) is intentional: it exercises the full pub/sub path end-to-end, the same pattern you'd use if the publisher and dashboard consumer were separate services.

---

## Features

- 📈 **Live market data** — real intraday quotes via `yfinance`, not simulated data
- 🔁 **Event-driven pipeline** — Solace PubSub+ direct messaging for publish/subscribe
- 🧵 **Thread-safe shared state** — lock-protected in-memory store bridges the background streamer and the Dash UI
- 📊 **Auto-refreshing dashboard** — chart redraws every second with no manual reload
- ⚙️ **Configurable via environment variables** — broker host, VPN, and credentials are all overridable
- 🛑 **Graceful shutdown support** — listens for a `quit` control topic to stop the receiver

---

## Tech Stack

| Component        | Technology                          |
|-------------------|--------------------------------------|
| Messaging broker  | Solace PubSub+ (Python API)         |
| Market data       | [yfinance](https://pypi.org/project/yfinance/) |
| Dashboard / UI    | [Dash](https://dash.plotly.com/) + Plotly |
| Language          | Python 3                            |
| Concurrency       | `threading` (daemon background thread) |

---

## Prerequisites

- Python 3.9+
- A running **Solace PubSub+ broker** — either:
  - [Solace PubSub+ Software Event Broker](https://solace.com/products/event-broker/software/getting-started/) running locally (e.g. via Docker), or
  - A [Solace Cloud](https://solace.com/products/event-broker/cloud/) free-tier instance

---

## Installation

```bash
# Clone the repo
git clone https://github.com/<neetiupadhye>/live-stock-dashboard-solace.git
cd live-stock-dashboard-solace

# (Recommended) create a virtual environment
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

---

## Configuration

The Solace connection is configured via environment variables, with sensible local defaults baked in:

| Variable            | Default                    | Description                          |
|----------------------|------------------------------|----------------------------------------|
| `SOLACE_HOST`        | `tcp://localhost:55554`     | Broker connection URI                 |
| `SOLACE_VPN`         | `default`                   | Message VPN name                      |
| `SOLACE_USERNAME`    | `admin`                     | Broker username                       |
| `SOLACE_PASSWORD`    | `admin`                     | Broker password                       |

Example (pointing at a Solace Cloud instance):

```bash
export SOLACE_HOST="tcps://<your-instance>.messaging.solace.cloud:55443"
export SOLACE_VPN="your-vpn-name"
export SOLACE_USERNAME="your-username"
export SOLACE_PASSWORD="your-password"
```

The ticker symbol and poll interval are configured directly at the top of `stock_price_streamer.py`:

```python
TICKER = "D05.SI"            # any valid Yahoo Finance ticker
POLL_INTERVAL_SECONDS = 15   # how often to poll for a new price
```

> The default ticker (`D05.SI`, DBS Group Holdings on SGX) is chosen so the market is open during typical SGT working hours. Swap it for `AAPL` or any other symbol if you're testing during US market hours instead.

---

## Usage

Run everything from the single entry point:

```bash
python3 main.py
```

This will:
1. Connect to Solace and start polling/publishing/receiving quotes on a background thread
2. Launch the dashboard at **http://127.0.0.1:8050**

Open that URL in a browser to watch the live price chart update in real time.

### Running components independently

Each module can also be run standalone for testing:

```bash
python3 data_store.py          # sanity-checks the thread-safe store
python3 dashboard.py           # runs just the dashboard (empty until data arrives)
python3 stock_price_streamer.py  # runs just the streamer/publisher
```

---

## Project Structure

```
.
├── main.py                   # Entry point — starts streamer thread + dashboard
├── stock_price_streamer.py   # Polls Yahoo Finance, publishes/receives via Solace
├── data_store.py             # Thread-safe in-memory store shared by both sides
├── dashboard.py               # Dash app that renders the live price chart
├── requirements.txt           # Python dependencies
└── README.md
```

---

## Design Notes

- **Why poll `history()` instead of `fast_info`?** Yahoo's lightweight quote endpoint (`fast_info`) is cached upstream and can return a stale price if polled faster than the cache refreshes. Pulling the latest 1-minute bar via `history(period="1d", interval="1m")` guarantees a fresh, real market-timestamped value.
- **Why deduplicate on price change?** Publishing only when the price actually moves avoids flooding the broker with duplicate ticks between 1-minute bar updates.
- **Why a lock around the shared store?** The streamer writes on a background thread while Dash reads on a timer on the main thread — the lock prevents any read/write race on the underlying `deque`s.

---

## Potential Improvements

- [ ] Add a `requirements.txt` / `pyproject.toml` for reproducible installs
- [ ] Support multiple tickers simultaneously, with a selector in the dashboard
- [ ] Persist historical data to a database instead of an in-memory `deque`
- [ ] Add unit tests for `data_store.py`
- [ ] Containerize with Docker Compose (app + local Solace broker)
- [ ] Add authentication/config via a `.env` file with `python-dotenv`
