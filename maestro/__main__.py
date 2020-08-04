from typing import Union, Any, Optional
import os
import traceback
import asyncio
import orjson
from nats.aio.client import Client as NATS

TO_STORE = {
    "_conthesis/watcher/UpdateWatcher": {
        "kind": "entwatcher.UpdateWatchEntity",
        "wildcard_triggers": ["_conthesis/watcher"],
        "properties": [
            {"name": "name", "kind": "META_FIELD", "value": "updated_entity"},
            {"name": "entity", "kind": "META_ENTITY", "value": "updated_entity",},
        ],
    }
}

ENTWATCHER_BOOTSTRAP_ACTION = {
    "meta": {
        "updated_entity": "_conthesis/watcher/UpdateWatcher",
        "bootstrap": True,
    },
    "action_source": "ENTITY",
    "action": "_conthesis.watcher.UpdateWatcher"
}

class Maestro:
    nc: NATS
    def __init__(self):
        self.nc = NATS()
        self.shutdown_f = asyncio.get_running_loop().create_future()

    async def setup(self):
        await self.nc.connect(os.environ["NATS_URL"])
        asyncio.create_task(self.manage_system())

    async def req(self, topic: str, data: Union[bytes, str]) -> bytes:
        bfr = data if isinstance(data, bytes) else data.encode("utf-8")
        res = await self.nc.request(topic, bfr, timeout=3)
        return res.data

    async def store_resource(self, entity: str, data):
        data_buf = orjson.dumps(data) if not isinstance(data, bytes) else data
        assignment = b"/entity/" + entity.encode("utf-8") + b"\n" + data_buf
        res = await self.req("conthesis.cfs.put", assignment)
        if res == b"ERR":
            raise RuntimeError("failed to store")
        return True

    async def get_resource(self, entity: str):
        return await self.req("conthesis.cfs.get", f"/entity/{entity}")

    async def ensure_resources(self):
        coros = [
                self.store_resource(k, v)
                for (k, v) in TO_STORE.items()
        ]
        await asyncio.gather(*coros)
        return True

    async def trigger_automatic_actions(self):
        res = await self.req("conthesis.action.TriggerAsyncAction", orjson.dumps(ENTWATCHER_BOOTSTRAP_ACTION))


    async def manage_system(self):
        while not self.shutdown_f.done():
            try:
                await self.ensure_resources()
                await self.trigger_automatic_actions()
                await self.self_test()
            except:
                traceback.print_exc()

            await asyncio.sleep(10)


    async def self_test(self):
        test_sequence = [f"self_test/{i}".encode("utf-8") for i in range(3)]
        test_results = []
        for x in test_sequence:
            await self.store_resource("_conthesis/self_test", x)
            res = await self.get_resource("_conthesis/self_test")
            if res != x:
                print(f"Expected {x}, was {res}")
            test_results.append(res == x)

        if not any(test_results):
            print("Self-tests failed completely")
        elif not all(test_results):
            fails = "".join(["P" if x else "F" for x in test_results])
            print(f"Self-test partial failure {fails}")



    async def shutdown(self):
        try:
            await self.nc.drain()
        finally:
            self.shutdown_f.set_result(True)

    async def wait_for_shutdown(self):
        await self.shutdown_f

async def main():
    m = Maestro()
    try:
        await m.setup()
        await m.wait_for_shutdown()
    finally:
        await m.shutdown()


asyncio.run(main())
