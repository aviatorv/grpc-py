import io

import dataframe_service_pb2
import dataframe_service_pb2_grpc
import grpc
import pandas as pd
import yfinance as yf


def run():
    ticker = "GOOG"
    goog_data = yf.download(ticker, period="max")

    buffer = io.BytesIO()
    goog_data.to_parquet(buffer, index=True)
    buffer.seek(0)

    with grpc.insecure_channel("localhost:50051") as channel:
        stub = dataframe_service_pb2_grpc.DataFrameServiceStub(channel)
        response = stub.SendDataFrame(dataframe_service_pb2.DataFrameRequest(data=buffer.read()))
        print("Server response:", response.message)


if __name__ == "__main__":
    run()
