import asyncio
import time
from typing import Any
from uuid import UUID

import arqanmode
import arqanmode.framework as af
from arqanmode.kafka import GenericConsumer
from arqanmode.storage import Storage


class Model(af.ModelInterface):

    def __init__(self):
        self.interface = None
        self.model = arqanmode.ModelV1(
            model_name="model_name_2",
            scheme=arqanmode.SchemeV1(
                name="schema_name",
                fields=[
                    arqanmode.SchemeFieldV1(
                        name="field_name_int",
                        value=arqanmode.SchemeFieldTypeEnum.Integer,
                    ),
                    arqanmode.SchemeFieldV1(
                        name="field_name_string",
                        value=arqanmode.SchemeFieldTypeEnum.String,
                    ),
                ]
            )
        )

    def get_interface(self) -> arqanmode.ModelV1:
        return self.model  # todo: make interface

    def parse_and_validate(self, raw_request: bytes) -> object:
        return {
            "field_name_int": 10,
            "field_name_string": "Alice",
        }

    async def process_task(self, input: object):
        assert input == {
            "field_name_int": 10,
            "field_name_string": "Alice",
        }

        time.sleep(5)

        return arqanmode.TaskResult(
            status=arqanmode.TaskStatus.SUCCESS,
            raw_response="Alice is 10".encode(),
        )


class TestStorage(Storage):
    async def get_task_status(self, task_id: UUID) -> arqanmode.TaskStatus | None:
        print("return task status", task_id)
        return arqanmode.TaskStatus.ENQUEUED

    async def save_task_status(self, task_id: UUID, status: arqanmode.TaskStatus):
        print("save task status", task_id, status)
        return

    async def save_task_result(self, task_id: UUID, reqs: list[str]):
        print("ok", task_id, reqs)

    async def ping(self):
        print("ping")
        return 200

    async def save_task_error(self, task_id: UUID, task_status: arqanmode.TaskStatus, *, err_code: str, err_msg: str,
                              err_details: Any | None = None):
        print("fail", task_id, task_status, err_code, err_msg, err_details)

    async def close(self):
        print("close connection")


model = Model()
storage = TestStorage()


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
        storage
    )

    print("framework created")

    await framework.process()

    print("processing stopped")

    await framework.stop()

    print("framework stopped")


if __name__ == "__main__":
    asyncio.run(main())
