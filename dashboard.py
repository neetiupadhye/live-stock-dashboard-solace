"""
dashboard.py

A live-updating Dash dashboard styled after the "Market Insight" trading
UI reference: dark theme, top nav bar, big price header, chart card with
timeframe/chart-type controls, and a sidebar stats panel.

This file only READS from data_store — it never writes to it.
The writing happens in subscriber.py.

Honesty note on data: the reference mockup shows fields this pipeline
doesn't actually have (Volume, Market Cap, 52-Week range, P/E, Div
Yield, Order Book, multi-timeframe history). Rather than fabricate
numbers for those, this dashboard shows them as "N/A" and disables the
controls (timeframes other than 1D, candlestick view) that would need
data we don't collect. Only Current Price, Open, High, and Low are
computed from real ticks in data_store, plus a moving average line
computed from that same real data.

Zoom: click-and-drag on the chart to box-zoom into a time range
(Plotly's default drag behavior). Double-clicking inside the chart
resets zoom back to the full view (native Plotly behavior).
`uirevision` on the figure keeps the zoom stable across the 1s
auto-refresh.
"""

from dash import Dash, dcc, html, Output, Input
import plotly.graph_objs as go

from data_store import data_store
from news_store import news_store
from solace_common import AVAILABLE_TICKERS, TICKER_INFO
from subscriber import switch_ticker

# --- Config -----------------------------------------------------------

# How often the chart redraws itself, in milliseconds.
REFRESH_INTERVAL_MS = 1000

# The ticker shown when the dashboard first loads — matches the topic
# the subscriber starts subscribed to (see main.py).
DEFAULT_TICKER = AVAILABLE_TICKERS[0]

def _ticker_display_info(ticker_symbol):
    """Look up display name/exchange/currency for a ticker, with a
    reasonable fallback for tickers not listed in TICKER_INFO."""
    return TICKER_INFO.get(
        ticker_symbol,
        {"name": ticker_symbol, "exchange": "—", "currency": "USD"},
    )


def _status_label_and_color(has_data, is_live):
    """
    Text/color for the LIVE badge, based on whether we've received any
    data yet for the selected ticker and whether the most recent point
    came from an open market. Distinguishes three states: still
    connecting, live, and market closed (last known price) — so a
    closed market like AAPL/MSFT outside US trading hours doesn't get
    mislabeled "LIVE" just because a price is being displayed.
    """
    if not has_data:
        return "CONNECTING…", COLOR_TEXT_MUTED
    if is_live:
        return "LIVE", COLOR_POSITIVE
    return "MARKET CLOSED", COLOR_TEXT_MUTED

# --- Palette (matches the reference design) ----------------------------

COLOR_BG = "#0a0e14"
COLOR_NAVBAR_BG = "#0d1218"
COLOR_CARD_BG = "#0f1720"
COLOR_BORDER = "rgba(255,255,255,0.06)"
COLOR_TEXT = "#e5e7eb"
COLOR_TEXT_MUTED = "rgba(229,231,235,0.55)"
COLOR_ACCENT_BLUE = "#3b82f6"
COLOR_ACCENT_BLUE_SOFT = "rgba(59,130,246,0.15)"
COLOR_POSITIVE = "#22c55e"
COLOR_NEGATIVE = "#ef4444"
COLOR_LINE = "#3b82f6"
COLOR_PILL_BG = "#1a2330"
COLOR_PILL_BG_DISABLED = "#141c26"


def _pill(label, active=False):
    """A small timeframe/tool pill button, styled but non-interactive
    for options we don't have the data to actually support."""
    return html.Div(
        label,
        style={
            "padding": "6px 12px",
            "borderRadius": "6px",
            "fontSize": "12px",
            "fontWeight": 700 if active else 500,
            "backgroundColor": COLOR_PILL_BG if active else "transparent",
            "color": COLOR_TEXT if active else COLOR_TEXT_MUTED,
            "cursor": "default" if active else "not-allowed",
            "opacity": 1 if active else 0.45,
            "userSelect": "none",
        },
        title="Live" if active else "Requires historical data this feed doesn't provide",
    )


def _stat_row(label, value_id=None, static_value=None, muted=False):
    """One row in the sidebar Overview panel. Either bound to a
    callback output (value_id) or a static placeholder (static_value)."""
    value_child = (
        html.Span(id=value_id, style={"float": "right", "fontWeight": 700, "color": COLOR_TEXT})
        if value_id
        else html.Span(
            static_value,
            style={"float": "right", "color": COLOR_TEXT_MUTED if muted else COLOR_TEXT, "fontWeight": 500},
        )
    )
    return html.Div(
        [label, value_child],
        style={
            "padding": "9px 0",
            "borderBottom": f"1px solid {COLOR_BORDER}",
            "fontSize": "13px",
            "color": COLOR_TEXT_MUTED,
        },
    )


def _news_item(article):
    """One row in the sidebar Latest News list: a clickable headline
    plus its publisher, styled to match the rest of the sidebar."""
    return html.Div(
        [
            html.A(
                article.get("title", "Untitled"),
                href=article.get("link") or "#",
                target="_blank",
                style={
                    "color": COLOR_TEXT,
                    "fontSize": "13px",
                    "fontWeight": 600,
                    "textDecoration": "none",
                    "lineHeight": "1.4",
                    "display": "block",
                },
            ),
            html.Div(
                article.get("publisher", ""),
                style={"color": COLOR_TEXT_MUTED, "fontSize": "11px", "marginTop": "4px"},
            ),
        ],
        style={"padding": "10px 0", "borderBottom": f"1px solid {COLOR_BORDER}"},
    )


def build_app():
    app = Dash(__name__)

    app.layout = html.Div(
        style={
            "backgroundColor": COLOR_BG,
            "minHeight": "100vh",
            "color": COLOR_TEXT,
            "fontFamily": "Inter, -apple-system, Arial, sans-serif",
        },
        children=[
            # --- Top nav bar ---------------------------------------------
            html.Div(
                style={
                    "backgroundColor": COLOR_NAVBAR_BG,
                    "padding": "14px 28px",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "space-between",
                    "borderBottom": f"1px solid {COLOR_BORDER}",
                },
                children=[
                    html.Div(
                        style={"display": "flex", "alignItems": "center", "gap": "6px"},
                        children=[
                            html.Div("MARKET", style={"fontWeight": 800, "letterSpacing": "1px", "color": COLOR_TEXT}),
                            html.Div("INSIGHT", style={"opacity": "0.6", "color": COLOR_TEXT, "fontWeight": 500}),
                        ],
                    ),
                    html.Div(
                        style={"display": "flex", "alignItems": "center", "gap": "28px"},
                        children=[
                            dcc.Dropdown(
                                id="ticker-dropdown",
                                options=[
                                    {
                                        "label": f"{t} — {_ticker_display_info(t)['name']}",
                                        "value": t,
                                    }
                                    for t in AVAILABLE_TICKERS
                                ],
                                value=DEFAULT_TICKER,
                                clearable=False,
                                searchable=False,
                                style={
                                    "width": "260px",
                                    "color": "#0a0e14",
                                    "fontSize": "13px",
                                },
                            ),
                            html.Div(
                                "Dashboard",
                                style={
                                    "color": COLOR_ACCENT_BLUE,
                                    "fontSize": "14px",
                                    "fontWeight": 600,
                                    "paddingBottom": "4px",
                                    "borderBottom": f"2px solid {COLOR_ACCENT_BLUE}",
                                },
                            ),
                            html.Div(
                                style={
                                    "width": "34px",
                                    "height": "34px",
                                    "borderRadius": "50%",
                                    "backgroundColor": "#2a3644",
                                    "display": "flex",
                                    "alignItems": "center",
                                    "justifyContent": "center",
                                    "fontSize": "13px",
                                    "fontWeight": 700,
                                    "color": COLOR_TEXT,
                                },
                                children="U",
                            ),
                        ],
                    ),
                ],
            ),

            # --- Header: name + live badge / big price + change ----------
            html.Div(
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "flex-end",
                    "padding": "24px 28px 0 28px",
                },
                children=[
                    html.Div(
                        children=[
                            html.Div(
                                [
                                    html.Span(id="company-name", style={"fontWeight": 800, "fontSize": "26px"}),
                                    html.Span(" "),
                                    html.Span(id="company-ticker", style={"fontWeight": 400, "fontSize": "26px", "color": COLOR_TEXT_MUTED}),
                                ]
                            ),
                            html.Div(
                                id="live-status-1",
                                style={"marginTop": "4px"},
                            ),
                        ]
                    ),
                    html.Div(
                        style={"textAlign": "right"},
                        children=[
                            html.Div(
                                [
                                    html.Span(id="summary-price", style={"fontSize": "30px", "fontWeight": 800}),
                                    html.Span(" "),
                                    html.Span(id="summary-change"),
                                ]
                            ),
                            html.Div(
                                id="live-status-2",
                                style={"fontSize": "12px", "color": COLOR_TEXT_MUTED, "marginTop": "4px"},
                            ),
                        ],
                    ),
                ],
            ),

            # --- Main content: chart card + sidebar -----------------------
            html.Div(
                style={
                    "display": "flex",
                    "gap": "24px",
                    "padding": "20px 28px 28px 28px",
                    "alignItems": "stretch",
                },
                children=[
                    # Left: chart card
                    html.Div(
                        style={
                            "flex": "1 1 0",
                            "backgroundColor": COLOR_CARD_BG,
                            "padding": "18px 20px",
                            "borderRadius": "14px",
                            "border": f"1px solid {COLOR_BORDER}",
                            "boxShadow": "0 8px 30px rgba(0,0,0,0.5)",
                            "display": "flex",
                            "flexDirection": "column",
                        },
                        children=[
                            html.Div(
                                style={
                                    "display": "flex",
                                    "justifyContent": "space-between",
                                    "alignItems": "center",
                                    "marginBottom": "14px",
                                    "flexWrap": "wrap",
                                    "gap": "10px",
                                    "flex": "0 0 auto",
                                },
                                children=[
                                    html.Div(
                                        [
                                            html.Span(id="chart-ticker-label", style={"fontWeight": 700, "fontSize": "13px"}),
                                            html.Span(id="chart-subtitle", style={"color": COLOR_TEXT_MUTED, "fontSize": "13px", "marginLeft": "8px"}),
                                        ]
                                    ),
                                    html.Div(
                                        style={"display": "flex", "gap": "4px", "alignItems": "center"},
                                        children=[
                                            _pill("1D", active=True),
                                            _pill("5D", active=True),
                                            html.Div(style={"width": "8px"}),
                                            _pill("Line", active=True),
                                        ],
                                    ),
                                ],
                            ),
                            html.Div(
                                # This wrapper is what actually grows to fill
                                # the leftover vertical space in the card
                                # (now that the card itself is stretched to
                                # match the sidebar's height). minHeight: 0
                                # is required here — without it, a flex
                                # child won't shrink below its content size,
                                # which would defeat the flex-grow above and
                                # let the graph overflow instead of filling.
                                style={"flex": "1 1 auto", "minHeight": "0"},
                                children=[
                                    dcc.Graph(
                                        id="price-chart",
                                        config={"displayModeBar": False},
                                        style={"height": "100%", "width": "100%"},
                                    ),
                                ],
                            ),
                            dcc.Interval(
                                id="interval-component",
                                interval=REFRESH_INTERVAL_MS,
                                n_intervals=0,
                            ),

                            html.Div(
                                "Disclaimer: Data is provided by Yahoo Finance and other content providers and may be delayed as specified by financial exchanges or other data providers",
                                style={"fontSize": "11px", "color": COLOR_TEXT_MUTED, "marginTop": "10px", "flex": "0 0 auto"},
                            ),
                        ],
                    ),

                    # Right: sidebar with stats (split into two separate boxes)
                    html.Div(
                        style={
                            "display": "flex",
                            "flexDirection": "column",
                            "gap": "12px",
                        },
                        children=[
                            # Separate small box for "test 123"
                            html.Div(
                                style={
                                    "width": "340px",
                                    "minWidth": "280px",
                                    "backgroundColor": COLOR_CARD_BG,
                                    "padding": "18px 20px",
                                    "borderRadius": "14px",
                                    "border": f"1px solid {COLOR_BORDER}",
                                    "height": "fit-content",
                                },
                                children=[
                                    html.Div("Welcome to the Stock Dashboard", style={"fontSize": "20px", "fontWeight": 700, "opacity": "0.90", "marginBottom": "8px"}),
                                    html.Div(
                                        children=[
                                            _stat_row("Here you can find the latest stock information and news.", value_id=""),
                                        ]
                                    ),
                                ],
                            ),
                            # Overview card (kept as its own box)
                            html.Div(
                                style={
                                    "width": "340px",
                                    "minWidth": "280px",
                                    "backgroundColor": COLOR_CARD_BG,
                                    "padding": "18px 20px",
                                    "borderRadius": "14px",
                                    "border": f"1px solid {COLOR_BORDER}",
                                    "height": "fit-content",
                                },
                                children=[
                                    html.Div("Overview", style={"fontSize": "20px", "fontWeight": 700, "opacity": "0.90", "marginBottom": "8px"}),
                                    html.Div(
                                        children=[
                                            _stat_row("Current Price", value_id="stat-current"),
                                            _stat_row("Open", value_id="stat-open"),
                                            _stat_row("High", value_id="stat-high"),
                                            _stat_row("Low", value_id="stat-low"),
                                        ]
                                    ),
                                ],
                            ),
                            # Latest News card — fed by news_store, which
                            # is filled by subscriber.py over Solace, same
                            # producer/consumer split as the price chart.
                            # This callback never touches yfinance itself.
                            html.Div(
                                style={
                                    "width": "340px",
                                    "minWidth": "280px",
                                    "backgroundColor": COLOR_CARD_BG,
                                    "padding": "18px 20px",
                                    "borderRadius": "14px",
                                    "border": f"1px solid {COLOR_BORDER}",
                                    "height": "fit-content",
                                },
                                children=[
                                    html.Div("Latest News", style={"fontSize": "20px", "fontWeight": 700, "opacity": "0.90", "marginBottom": "10px"}),
                                    html.Div(id="news-list"),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    @app.callback(
        Output("company-name", "children"),
        Output("company-ticker", "children"),
        Output("chart-ticker-label", "children"),
        Input("ticker-dropdown", "value"),
    )
    def on_ticker_change(selected_ticker):
        # Re-point the subscriber's live subscription at the newly
        # selected ticker's topic. If the subscriber isn't up yet
        # (e.g. this fires during initial page load before the
        # background thread finishes connecting) this just no-ops;
        # the chart/stat callbacks below will simply show "Waiting
        # for data..." until data for this ticker arrives.
        switch_ticker(selected_ticker)

        info = _ticker_display_info(selected_ticker)
        return info["name"], f"({selected_ticker})", selected_ticker

    @app.callback(
        Output("price-chart", "figure"),
        Output("chart-subtitle", "children"),
        Input("interval-component", "n_intervals"),
        Input("ticker-dropdown", "value"),
    )
    def update_chart(n_intervals, selected_ticker):
        dates, currents = data_store.get_data(selected_ticker)
        info = _ticker_display_info(selected_ticker)

        traces = [
            go.Scatter(
                x=dates,
                y=currents,
                mode="lines",
                name="Price",
                line=dict(color=COLOR_LINE, width=2.5),
                hovertemplate="%{x|%H:%M}<br>$%{y:.2f}<extra></extra>",
            )
        ]

        figure = go.Figure(
            data=traces,
            layout=go.Layout(
                # Keeps any zoom/pan the user has done in place across the
                # 1s interval refreshes. Plotly only preserves zoom state
                # between re-renders when uirevision stays the same value;
                # tying it to the ticker means switching tickers still
                # resets to the full view (new data), but repeated redraws
                # of the *same* ticker won't fight the user's zoom.
                uirevision=selected_ticker,
                margin=dict(l=50, r=50, t=10, b=40),
                plot_bgcolor=COLOR_CARD_BG,
                paper_bgcolor=COLOR_CARD_BG,
                font=dict(color=COLOR_TEXT),
                xaxis=dict(
                    title="Time",
                    gridcolor=COLOR_BORDER,
                    color=COLOR_TEXT_MUTED,
                    tickformat="%H:%M",
                ),
                yaxis=dict(
                    title=f"Price ({info['currency']})",
                    gridcolor=COLOR_BORDER,
                    color=COLOR_TEXT_MUTED,
                    side="right",
                    automargin=True,
                ),
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, font=dict(size=11)),
            ),
        )

        if currents:
            day_high = max(currents)
            day_low = min(currents)
            last_price = currents[-1]

            for value, color in (
                (day_high, COLOR_POSITIVE),
                (day_low, COLOR_NEGATIVE),
                (last_price, COLOR_TEXT),
            ):
                figure.add_hline(
                    y=value,
                    line=dict(color=color, width=2, dash="dash"),
                )
                figure.add_annotation(
                    xref="paper",
                    x=1.0,
                    xanchor="left",
                    y=value,
                    yref="y",
                    yanchor="middle",
                    text=f"{value:.2f}",
                    showarrow=False,
                    bgcolor=color,
                    font=dict(color=COLOR_BG, size=10, family="Roboto, Helvetica Neue, Arial, sans-serif"),
                    borderpad=3,
                    )
        
        subtitle = dates[-1][:10] if dates else "Waiting for data..."
        return figure, subtitle

        subtitle = dates[-1][:10] if dates else "Waiting for data..."
        return figure, subtitle

    @app.callback(
        Output("stat-current", "children"),
        Output("stat-open", "children"),
        Output("stat-high", "children"),
        Output("stat-low", "children"),
        Output("summary-price", "children"),
        Output("summary-change", "children"),
        Output("live-status-1", "children"),
        Output("live-status-2", "children"),
        Input("interval-component", "n_intervals"),
        Input("ticker-dropdown", "value"),
    )
    def update_stats(n, selected_ticker):
        dates, currents = data_store.get_data(selected_ticker)
        is_live = data_store.get_is_live(selected_ticker)
        info = _ticker_display_info(selected_ticker)
        status_label, status_color = _status_label_and_color(bool(currents), is_live)

        live_status_1 = [
            html.Span("● ", style={"color": status_color, "fontSize": "10px"}),
            html.Span(status_label, style={"color": status_color, "fontWeight": 700, "fontSize": "12px"}),
            html.Span("  ·  ", style={"color": COLOR_TEXT_MUTED, "fontSize": "12px"}),
            html.Span(info["exchange"], style={"color": COLOR_TEXT_MUTED, "fontSize": "12px"}),
        ]
        live_status_2 = f"{status_label} · {info['exchange']}"

        if not currents:
            waiting = html.Span("Waiting for data...", style={"color": COLOR_TEXT_MUTED, "fontSize": "14px", "fontWeight": 400})
            return "-", "-", "-", "-", "-", waiting, live_status_1, live_status_2

        current = currents[-1]
        open_price = currents[0]
        high = max(currents)
        low = min(currents)
        fmt = lambda v: f"${v:,.2f}"

        diff = current - open_price
        pct = (diff / open_price * 100) if open_price else 0.0
        sign = "+" if diff >= 0 else "-"
        color = COLOR_POSITIVE if diff >= 0 else COLOR_NEGATIVE
        change_component = html.Span(
            f"{sign}{abs(diff):.2f} ({sign}{abs(pct):.2f}%)",
            style={"color": color, "fontWeight": 700, "fontSize": "16px"},
        )

        return fmt(current), fmt(open_price), fmt(high), fmt(low), fmt(current), change_component, live_status_1, live_status_2

    @app.callback(
        Output("news-list", "children"),
        Input("interval-component", "n_intervals"),
        Input("ticker-dropdown", "value"),
    )
    def update_news(n_intervals, selected_ticker):
        # Pure local read — news_store is filled by subscriber.py over
        # Solace (see publisher.py's periodic + backfill-triggered news
        # publishing), so this never blocks on a network call itself,
        # even though it rides the same 1s interval as the price chart.
        articles = news_store.get(selected_ticker)
        if not articles:
            return html.Div("Waiting for news...", style={"color": COLOR_TEXT_MUTED, "fontSize": "13px"})
        return [_news_item(article) for article in articles]

    return app


def run_dashboard():
    app = build_app()
    app.run(debug=False)


if __name__ == "__main__":
    run_dashboard()
