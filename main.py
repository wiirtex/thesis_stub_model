import asyncio
import random

import arqanmode
import arqanmode.framework as af
import pydantic
import time
from arqanmode.framework import ModelRegistryClient
from arqanmode.kafka import GenericConsumer
from arqanmode.storage import RedisClient


class Model(af.ModelInterface):

    def __init__(self):
        self.interface = arqanmode.ModelV1(
            model_name="test_model",
            scheme=arqanmode.SchemeV1(
                name="test_schema_name",
                fields=[
                    arqanmode.SchemeFieldV1(
                        name="test_field_name",
                        value=pydantic.StrictStr,
                    ),
                ]
            )
        )

    def get_interface(self) -> arqanmode.ModelV1:
        return self.interface

    def parse_and_validate(self, raw_request: bytes) -> object:
        obj = self.interface.parse_raw(raw_request)
        return {
            "name": obj.scheme.fields[0].val,
        }

    async def process_task(self, input: object):
        time.sleep(random.randint(1, 30))  # simulation of the process of data processing

        return arqanmode.TaskResult(
            status=arqanmode.TaskStatus.SUCCESS,
            raw_response=f"hello, {input['name']}!".encode(),
        )


model = Model()


async def main():
    print("start main")

    framework = af.ModelFramework(
        model,
        GenericConsumer.Config(
            topic="test",
            group_id="test",
            server="localhost:9092",
            secure=False,
            username="",
            password="",
            ca_file="",
        ),
        RedisClient.Config(
            url='redis://localhost',
            port='6379',
            secure=False,
            password=None,
            ca_file=None,
        ),
        ModelRegistryClient.Config(
            url="localhost",
            port=8000,
        ),
    )

    print("framework created")

    await framework.process()

    print("processing stopped")

    await framework.stop()

    print("framework stopped")


if __name__ == "__main__":
    asyncio.run(main())
