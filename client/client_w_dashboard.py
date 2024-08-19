import io
import threading

import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import dataframe_service_pb2
import dataframe_service_pb2_grpc
import grpc
import pandas as pd
from dash import Dash, Input, Output, dcc, html

app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])


equities = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "AMZN": "Amazon",
    "GOOGL": "Alphabet",
    "TSLA": "Tesla",
}


data = {
    "ticker": [ticker for ticker in equities],
    "company": [name for name in equities.values()],
    "quantity": [75, 40, 100, 50, 40],
    "price": [150, 220, 3300, 2800, 720],
    "previous_price": [0, 0, 0, 0, 0],
}
df = pd.DataFrame(data)


columnDefs = [
    {"headerName": "Stock Ticker", "field": "ticker", "filter": True},
    {"headerName": "Company", "field": "company", "filter": True},
    {"headerName": "Shares", "field": "quantity", "editable": True, "type": "rightAligned"},
    {
        "headerName": "Last Close Price",
        "field": "price",
        "type": "rightAligned",
        "valueFormatter": {"function": "d3.format('$,.2f')(params.value)"},
        "cellRenderer": "agAnimateShowChangeCellRenderer",
        "cellClassRules": {
            "price-increase": "x > previous_price",
            "price-decrease": "x < previous_price",
        },
    },
    {
        "headerName": "Market Value",
        "type": "rightAligned",
        "valueGetter": {"function": "Number(params.data.price) * Number(params.data.quantity)"},
        "valueFormatter": {"function": "d3.format('$,.2f')(params.value)"},
        "cellRenderer": "agAnimateShowChangeCellRenderer",
    },
]

defaultColDef = {
    "filter": "agNumberColumnFilter",
    "resizable": True,
    "sortable": True,
    "editable": False,
    "floatingFilter": True,
    "minWidth": 125,
}

grid = dag.AgGrid(
    id="portfolio-grid",
    className="ag-theme-alpine-dark",
    columnDefs=columnDefs,
    rowData=df.to_dict("records"),
    columnSize="sizeToFit",
    defaultColDef=defaultColDef,
    dashGridOptions={"undoRedoCellEditing": True, "rowSelection": "single"},
)

app.layout = dbc.Container(
    [
        html.Div("My Portfolio", className="h2 p-2 text-white bg-primary text-center"),
        dbc.Row(dbc.Col(grid, className="py-4")),
        dcc.Interval(id="interval-component", interval=1 * 1000, n_intervals=0),  # Update every 5 seconds
    ]
)


def start_grpc_stream_updates(stub):
    feed_id = "google_stock_feed"
    job_id = "job_12345"

    update_stream = stub.StreamUpdates(dataframe_service_pb2.DataFrameRequest(feed_id=feed_id, job_id=job_id))

    for update in update_stream:
        buffer = io.BytesIO(update.data)
        update_df = pd.read_parquet(buffer)
        yield update_df


def grpc_update_listener(grid_data):
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = dataframe_service_pb2_grpc.DataFrameServiceStub(channel)
        for update_df in start_grpc_stream_updates(stub):
            for index, row in update_df.iterrows():
                ticker = row["ticker"]
                grid_data.loc[grid_data["ticker"] == ticker, "previous_price"] = grid_data.loc[
                    grid_data["ticker"] == ticker, "price"
                ]
                grid_data.loc[grid_data["ticker"] == ticker, "price"] = row["price"]


grid_data = df.copy()

# Start the gRPC streaming in a background thread
grpc_thread = threading.Thread(target=grpc_update_listener, args=(grid_data,), daemon=True)
grpc_thread.start()


# Callback to update the ag-Grid with the latest data
@app.callback(
    Output("portfolio-grid", "rowData"),
    Input("interval-component", "n_intervals"),
)
def update_grid(n):
    return grid_data.to_dict("records")


if __name__ == "__main__":
    app.run_server(debug=True)
