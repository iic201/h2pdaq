from src.central import CentralDAQService
import asyncio
import grpc
from h2pcontrol.central_daq.v1.central_daq_pb2_grpc import (
    add_CentralDAQServiceServicer_to_server,
)

async def serve() -> None:
    server = grpc.aio.server(
        options=[
            ("grpc.max_receive_message_length", 50 * 1024 * 1024),
            ("grpc.max_send_message_length", 50 * 1024 * 1024),
        ]
    )

    service = CentralDAQService()
    await service.start()

    add_CentralDAQServiceServicer_to_server(service, server)

    server.add_insecure_port("[::]:50052")
    await server.start()

    print("[Central DAQ] listening on :50052..")

    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        pass
    finally:
        print("[Central DAQ] Shutting down..")
        try:
            await service.stop()
            await server.stop(5)
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass