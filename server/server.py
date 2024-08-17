import io
from concurrent import futures

import dataframe_service_pb2
import dataframe_service_pb2_grpc
import grpc
import pandas as pd


class DataFrameServiceServicer(dataframe_service_pb2_grpc.DataFrameServiceServicer):
    def SendDataFrame(self, request, context):
        feed_id = request.feed_id
        job_id = request.job_id
        print(f"Received request with feed_id: {feed_id}, job_id: {job_id}")
        buffer = io.BytesIO(request.data)
        df = pd.read_parquet(buffer)

        print("Received DataFrame:")
        print(df)

        return dataframe_service_pb2.DataFrameResponse(message="DataFrame received successfully!")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dataframe_service_pb2_grpc.add_DataFrameServiceServicer_to_server(DataFrameServiceServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Server started on port 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
