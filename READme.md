# Live Stock Price Streamer & Dashboard

A real-time, multi-stock price pipeline built on **Solace PubSub+** event streaming, with live market data pulled from **Yahoo Finance** and visualized in an auto-refreshing **Dash** web dashboard.

The project demonstrates an end-to-end event-driven architecture: a standalone publisher polls market data and publishes it onto a Solace message broker, a standalone subscriber consumes it back off the broker into a thread-safe in-memory store, and a Dash dashboard reads that store to drive a live-updating chart — with a dropdown that lets you switch which stock you're watching at any time.

---

## Architecture

```
┌─────────────────┐      ┌───────────────────┐          ┌─────────────────┐
│  Yahoo Finance  │      │   Solace PubSub+  │          │  Dash Web App   │
│  (yfinance API) │      │   Event Broker    │          │ (localhost:8050)│
└────────┬────────┘      └─────────┬─────────┘          └───────┬─────────┘
         │ poll every 15s          │                            │
         ▼                         │                            │
┌──────────────────┐   publish     │                            │
│  publisher.py    ├───────────────►                            │
│  (standalone     │  backfill     │                            │
│   process)       │◄ requests ────┤                            │
└──────────────────┘               │                            │
                                   │  subscribe                 │
                      ┌────────────┴─────────┐                  │
                      │   subscriber.py      │                  │
                      │ (one topic at a time)│                  │
                      └───────────┬──────────┘                  │
                                  │ write                       │
                                  ▼                             │
                        ┌──────────────────┐   read (1x/sec)    │
                        │   data_store.py  ├────────────────────►
                        │ (thread-safe,    │                dashboard.py
                        │  per-ticker)     │
                        └──────────────────┘
```

`main.py` wires together the **receiving** side of the pipeline: it starts `subscriber.py` on a background thread and runs the Dash server (`dashboard.py`) on the main thread. Both share a single `data_store` instance in memory, guarded by a lock, so producer and consumer never race.

`publisher.py` is a fully separate, standalone process — it can run on the same machine or a completely different one, as long as it points (via the `SOLACE_*` env vars) at the same broker the subscriber is connected to.

---

## How It Works

1. **`publisher.py`** polls Yahoo Finance every 15 seconds for the latest 1-minute price bar of every ticker in `AVAILABLE_TICKERS`, and publishes any new bar as a JSON message to that ticker's own Solace topic (`solace/samples/python/stocks/<TICKER>`). On startup it also replays each ticker's full day-so-far history so a dashboard opened mid-session isn't starting from an empty chart.
2. **`subscriber.py`** connects to the same broker and subscribes to exactly **one** ticker's topic at a time — whichever one the dashboard currently has selected — and writes every received data point into the shared `data_store`.
3. When you pick a different stock in the dashboard's dropdown, `subscriber.switch_ticker()` re-points the live subscription at the new ticker's topic **and** sends a backfill request to the publisher (on a separate request topic), asking it to replay that ticker's day-so-far history. This gives every stock the same "full history, then live" experience the first ticker gets at startup, instead of building the chart up from scratch on live ticks alone.
4. **`data_store.py`** keeps a separate, bounded series of points per ticker (not just the one currently selected), so switching back to a stock you looked at earlier doesn't lose what was already collected for it.
5. **`dashboard.py`** runs a Dash app styled as a dark-themed trading UI. It polls `data_store` once per second for the selected ticker and redraws the live price chart, plus Current/Open/High/Low stats and a moving-average overlay — all computed from real ticks. Fields the data doesn't actually support (Volume, Market Cap, 52-Week range, P/E, multi-timeframe candles, etc.) are shown as "N/A" or disabled rather than faked.
6. **`main.py`** is the entry point for the receiving side — it starts the subscriber on a background thread and then blocks on the Dash server. The publisher is started separately.

This publish → receive → store path (rather than the dashboard reading Yahoo Finance directly) is intentional: it exercises the full pub/sub path end-to-end, the same pattern you'd use if the publisher and dashboard consumer were genuinely separate services on separate machines.

---

## Features

- 📈 **Live market data** — real intraday quotes via `yfinance`, not simulated data
- 🔀 **Multi-stock support** — switch between tickers live via a dropdown, with automatic history backfill on every switch
- 🔁 **Event-driven pipeline** — Solace PubSub+ direct messaging for publish/subscribe, with a dedicated backfill-request topic for on-demand history replay
- 🧵 **Thread-safe, per-ticker shared state** — lock-protected in-memory store holds history for every ticker you've visited, not just the current one
- 📊 **Auto-refreshing dashboard** — dark-themed "Market Insight" style UI that redraws every second with no manual reload
- 🟢 **Live vs. closed-market awareness** — each tick is tagged live/closed based on bar freshness, so a closed market shows "MARKET CLOSED" instead of a misleading "LIVE" badge
- ⚙️ **Configurable via environment variables** — broker host, VPN, and credentials are all overridable
- 🧩 **Independently deployable** — publisher and subscriber/dashboard can run on different machines, as long as both reach the same Solace broker

---

## Tech Stack

| Component        | Technology                                     |
|------------------|------------------------------------------------|
| Messaging broker | Solace PubSub+ (Python API)                    |
| Market data      | [yfinance](https://pypi.org/project/yfinance/) |
| Dashboard / UI   | [Dash](https://dash.plotly.com/) + Plotly      |
| Language         | Python 3                                       |
| Concurrency      | `threading` (daemon background thread)         |

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

| Variable             | Description                          |
|----------------------|--------------------------------------|
| `SOLACE_HOST`        | Broker connection URI                |
| `SOLACE_VPN`         | Message VPN name                     |
| `SOLACE_USERNAME`    | Broker username                      |
| `SOLACE_PASSWORD`    | Broker password                      |

Example (pointing at a Solace Cloud instance):

```bash
export SOLACE_HOST="tcps://<your-instance>.messaging.solace.cloud:55443"
export SOLACE_VPN="your-vpn-name"
export SOLACE_USERNAME="your-username"
export SOLACE_PASSWORD="your-password"
```

Set these identically wherever `publisher.py` and `subscriber.py`/`main.py` run, so they meet on the same broker.

The set of tradeable tickers, their display info, and the poll interval are configured in `solace_common.py` and `publisher.py`:

```python
# solace_common.py
AVAILABLE_TICKERS = ["D05.SI", "O39.SI", "AAPL", "MSFT"]   # any valid Yahoo Finance tickers

# publisher.py
POLL_INTERVAL_SECONDS = 15   # how often each ticker is polled for a new price
```

> The default first ticker (`D05.SI`, DBS Group Holdings on SGX) is what the dashboard's dropdown and the subscriber's initial subscription both default to on startup. Add or remove tickers freely — both the publisher and the dashboard's dropdown pick up the list automatically.

---

## Usage

Publisher and subscriber/dashboard are run as two separate processes.

**1. Start the publisher** (feeds the broker with data — can run on any machine that can reach the broker):

```bash
python3 publisher.py
```

**2. Start the subscriber + dashboard** (in another terminal, or on another machine):

```bash
python3 main.py
```

This will:
1. Connect to Solace and start receiving quotes for the default ticker on a background thread
2. Launch the dashboard at **http://127.0.0.1:8050**

Open that URL in a browser to watch the live price chart update in real time. Use the dropdown in the top nav bar to switch stocks — the subscriber will re-subscribe and request a fresh history backfill for whichever one you pick.

### Running components independently

Each module can also be run standalone for testing:

```bash
python3 data_store.py     # sanity-checks the thread-safe, per-ticker store
python3 dashboard.py      # runs just the dashboard (empty until data arrives)
python3 subscriber.py     # runs just the subscriber, sitting on the default ticker
python3 publisher.py      # runs just the publisher
```

---

## Project Structure

```
.
├── main.py                # Entry point for the receiving side — starts subscriber thread + dashboard
├── publisher.py            # Standalone: polls Yahoo Finance, publishes quotes + serves backfill requests
├── subscriber.py            # Standalone: subscribes to one ticker at a time, writes into data_store
├── solace_common.py          # Shared broker connection setup, topic naming, and ticker config
├── data_store.py              # Thread-safe, per-ticker in-memory store shared by subscriber and dashboard
├── dashboard.py                 # Dash app that renders the live price chart and stats
├── requirements.txt              # Python dependencies
└── README.md
```

---

## Design Notes

- **Why poll `history()` instead of `fast_info`?** Yahoo's lightweight quote endpoint (`fast_info`) is cached upstream and can return a stale price if polled faster than the cache refreshes. Pulling the latest 1-minute bar via `history(period="1d", interval="1m")` guarantees a fresh, real market-timestamped value.
- **Why publish per-ticker, not per-message-sequence, topics?** One topic per ticker (rather than embedding a sequence number in the path) lets the subscriber subscribe to exactly one stock's topic at a time, and cleanly swap subscriptions when the dashboard's dropdown changes.
- **Why a separate backfill-request topic?** Switching tickers needs the same "full day so far, then live" experience the first ticker gets at startup. Rather than have the subscriber re-fetch history itself, it asks the publisher (which already owns the yfinance fetch logic) to replay it — keeping data-fetching logic in one place.
- **Why keep data for every ticker, not just the selected one?** So switching back to a stock you already looked at doesn't lose what was collected for it, even though only one ticker is actively subscribed to live updates at a time.
- **Why a lock around the shared store?** The subscriber writes on its own receiver thread while Dash reads on a timer on the main thread — the lock prevents any read/write race on the underlying per-ticker `deque`s.
- **Why show "N/A" for some dashboard fields?** The reference UI this dashboard is styled after shows fields (Volume, Market Cap, 52-Week range, P/E, Dividend Yield, Order Book, multi-timeframe candles) that this pipeline doesn't actually collect. Rather than fabricate numbers, those fields are shown as "N/A" and the controls that would need them are disabled.

---

## Potential Improvements

- [ ] Add a `pyproject.toml` for reproducible installs alongside `requirements.txt`
- [ ] Persist historical data to a database instead of an in-memory `deque`
- [ ] Add unit tests for `data_store.py` and the backfill request/reply flow
- [ ] Containerize with Docker Compose (publisher + subscriber/dashboard + local Solace broker)
- [ ] Add authentication/config via a `.env` file with `python-dotenv`
- [ ] Support watching more than one ticker at once (e.g. a multi-chart or watchlist view) instead of one-at-a-time
