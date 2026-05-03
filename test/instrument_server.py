"""
Instrument server example that runs DAQPipeline without UI subscribers.

This module demonstrates how to decorate simple instrument operations
(sum and multiplication) and persist events via the pipeline.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from daq_pipeline import DAQPipeline, capture


@dataclass(slots=True)
class MathRequest:
    a: float
    b: float

    def to_dict(self) -> dict[str, float]:
        return {"a": self.a, "b": self.b}


@dataclass(slots=True)
class MathReply:
    result: float
    op: str

    def to_dict(self) -> dict[str, float | str]:
        return {"result": self.result, "op": self.op}


class InstrumentServer:
    def __init__(self) -> None:
        self.pipeline = DAQPipeline(
            run_id="instrument_demo_run",
            hdf5_path="./instrument_demo_data.h5",
            influx_url="http://localhost:8181",
            influx_token="apiv3_EaIZSHuWGmXAVZQ7sjXvg4Adea9-72u2OkCUt1XE9w_w-9ZC0mSPxVvCYYJCNc-PZteM0fFwyQq3KglwEWwc1g",
            influx_org="beyerlab",
            influx_bucket="test",
        )

    async def start(self) -> None:
        await self.pipeline.start()

    async def stop(self) -> None:
        await self.pipeline.stop()

    @capture(source="instrument", direction="both")
    async def sum_values(self, request: MathRequest) -> MathReply:
        return MathReply(result=request.a + request.b, op="sum")

    @capture(source="instrument", direction="both")
    async def multiply_values(self, request: MathRequest) -> MathReply:
        return MathReply(result=request.a * request.b, op="multiply")


async def main() -> None:
    server = InstrumentServer()
    await server.start()

    # Intentionally no UI subscribers are registered.
    try:
        reqs = [
            MathRequest(2, 3),
            MathRequest(4, 5),
            MathRequest(1.5, 10),
        ]

        for req in reqs:
            sum_reply = await server.sum_values(req)
            mul_reply = await server.multiply_values(req)
            print(f"sum({req.a}, {req.b}) = {sum_reply.result}")
            print(f"multiply({req.a}, {req.b}) = {mul_reply.result}")

        await asyncio.sleep(1.0)
        print("Pipeline stats:", server.pipeline.stats)
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
