"""
dashboard.py

A live-updating Dash dashboard that plots the stock's current price
over time, reading from the shared data_store as new data arrives.

This file only READS from data_store — it never writes to it.
The writing happens in stock_price_streamer.py.
"""

from dash import Dash, dcc, html, Output, Input
import plotly.graph_objs as go

from data_store import data_store

# How often the chart redraws itself, in milliseconds.
# 1000 = redraw once per second.
REFRESH_INTERVAL_MS = 1000


def build_app():
    app = Dash(__name__)

    app.layout = html.Div([
        html.H2("Live Stock Current Price"),

        dcc.Graph(id="price-chart"),

        # This invisible component just fires a timer tick every
        # REFRESH_INTERVAL_MS — that tick is what triggers the
        # callback below to redraw the chart.
        dcc.Interval(id="interval-component", interval=REFRESH_INTERVAL_MS, n_intervals=0)
    ])

    @app.callback(
        Output("price-chart", "figure"),
        Input("interval-component", "n_intervals")
    )
    def update_chart(n_intervals):
        dates, currents = data_store.get_data()

        figure = go.Figure(
            data=[
                go.Scatter(
                    x=dates,
                    y=currents,
                    mode="lines",
                    name="Current Price"
                )
            ],
            layout=go.Layout(
                xaxis_title="Market Time",   # this is the actual timestamp of each price bar, not when we polled for it — the two can differ by a few minutes
                yaxis_title="Current Price",
                margin=dict(l=40, r=20, t=20, b=40)
            )
        )
        return figure

    return app


def run_dashboard():
    app = build_app()
    # debug=False is important here since this will run inside a
    # background-threaded setup later (main.py) — Dash's debug/reloader
    # mode doesn't play well with that.
    app.run(debug=False)


if __name__ == "__main__":
    # Lets you test the dashboard on its own, even before data_store
    # has any real data in it (chart will just be empty until then).
    run_dashboard()
