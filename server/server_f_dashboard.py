import io
import random
import time
from concurrent import futures

import dataframe_service_pb2
import dataframe_service_pb2_grpc
import grpc
import pandas as pd

equities = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "AMZN": "Amazon",
    "GOOGL": "Alphabet",
    "TSLA": "Tesla",
}


class DataFrameServiceServicer(dataframe_service_pb2_grpc.DataFrameServiceServicer):
    def __init__(self):
        # Assume we have an initial large DataFrame stored on the server
        self.df = pd.DataFrame({"col1": range(10000), "col2": range(10000, 20000)})

    def GetInitialDataFrame(self, request, context):
        feed_id = request.feed_id
        job_id = request.job_id
        print(f"Received request for initial DataFrame with feed_id: {feed_id}, job_id: {job_id}")

        buffer = io.BytesIO()
        self.df.to_parquet(buffer, index=True)
        buffer.seek(0)
        return dataframe_service_pb2.DataFrameResponse(
            message="Initial DataFrame sent successfully!", data=buffer.read()
        )

    def StreamUpdates(self, request, context):
        feed_id = request.feed_id
        job_id = request.job_id
        print(f"Streaming updates for feed_id: {feed_id}, job_id: {job_id}")

        # Simulating mini-batch or single row updates
        for i in range(1, 10020):  # Let's say these are the updates
            # Create a mini-batch update
            update_df = pd.DataFrame(
                {
                    "ticker": [random.choice(list(equities.keys()))],
                    "price": [150.23 + i], 
                }
            )

            buffer = io.BytesIO()
            update_df.to_parquet(buffer, index=True)
            buffer.seek(0)

            time.sleep(0.2)

            yield dataframe_service_pb2.DataFrameUpdate(feed_id=feed_id, job_id=job_id, data=buffer.read())


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dataframe_service_pb2_grpc.add_DataFrameServiceServicer_to_server(DataFrameServiceServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Server started on port 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
