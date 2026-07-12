# Live Stock Price Visualiser (Solace PubSub+ + Dash)

A real-time stock price dashboard that streams historical daily price
data through a **Solace PubSub+** event broker and visualises it live
using **Plotly Dash**.

## How it works

```
stock_price_streamer.py  --publishes-->  Solace Broker  --delivers-->  stock_price_streamer.py (receiver)
                                                                              |
                                                                              v
                                                                        data_store.py
                                                                        (thread-safe buffer)
                                                                              |
                                                                              v
                                                                        dashboard.py
                                                                     (live Dash chart, redraws every 1s)
```

- **`stock_price_streamer.py`** — Connects to a Solace broker, acts as both
  publisher and subscriber. It streams historical daily OHLC data (from the
  [`paperswithbacktest/Stocks-Daily-Price`](https://huggingface.co/datasets/paperswithbacktest/Stocks-Daily-Price)
  dataset on Hugging Face) as simulated "live" ticks, one row per second.
- **`data_store.py`** — A small thread-safe in-memory store (`deque` +
  `threading.Lock`) that bridges the background streaming thread and the
  main dashboard thread.
- **`dashboard.py`** — A Dash app that polls `data_store` every second and
  redraws a live line chart of the stock's opening price.
- **`main.py`** — Entry point. Starts the streamer on a background thread
  and the dashboard on the main thread.

## Getting started

### 1. Prerequisites
- Python 3.9+
- A running Solace PubSub+ broker (the free
  [Solace PubSub+ software broker](https://solace.com/products/event-broker/software/getting-started/)
  or a cloud instance both work)

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure broker connection
Set these environment variables (defaults shown are for a local broker):

| Variable          | Default                  |
|--------------------|---------------------------|
| `SOLACE_HOST`      | `enter your host details` |
| `SOLACE_VPN`       | `enter vpn name`          |
| `SOLACE_USERNAME`  | `enter username`          |
| `SOLACE_PASSWORD`  | `enter password`          |

### 4. Run
```bash
python3 main.py
```
Then open **http://127.0.0.1:8050** in your browser to see the live chart.
You'll be prompted in the terminal to enter a name (used to identify the
publisher session).

## Project structure
```
.
├── main.py                   # Entry point — starts streamer + dashboard
├── stock_price_streamer.py   # Solace publisher/subscriber logic
├── data_store.py             # Thread-safe shared data buffer
├── dashboard.py              # Live Dash visualisation
└── requirements.txt
```

## Notes
- The streamer publishes and subscribes to the *same* broker in one process,
  simulating a real-time feed from historical data for demo purposes.
- `data_store` caps memory usage by only retaining the most recent 500 points.
- Dash runs with `debug=False` since the app runs alongside a background
  thread — Dash's reloader doesn't play well with that setup.
