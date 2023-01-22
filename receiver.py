import os
import json
import asyncio
import subprocess
from starlette.requests import Request

import ray
from ray import serve
from ray.experimental.state.api import list_actors


@serve.deployment(
    num_replicas=2,
    ray_actor_options={"num_cpus": 1},
)
class Receiver:
    def __init__(self, node_killer_handle):
        self.node_killer_handle = node_killer_handle
        print(
            f"Receiver actor starting on node {ray.get_runtime_context().get_node_id()}"
        )

    async def __call__(self, request: Request):
        request_json = await request.json()
        kill_node = request_json.get("kill_node", "False")
        if kill_node == "True":
            print("Received kill request. Attempting to kill a node.")
            await asyncio.wait(
                [
                    asyncio.wait(
                        [self.node_killer_handle.kill_node.remote()], timeout=10
                    )
                ],
                timeout=10,
            )
        return f"(PID: {os.getpid()}) Received request!"


@serve.deployment(num_replicas=1, ray_actor_options={"num_cpus": 0})
class NodeKiller:
    def kill_node(self):
        try:
            actors = list_actors(filters=[("state", "=", "ALIVE")], timeout=3)
            print(f"Actor summary:\n{json.dumps(actors, indent=4)}")
        except Exception as e:
            print(f"Failed to get actor info. Got exception\n{e}")
        print(f"Killing node {ray.get_runtime_context().get_node_id()}")
        subprocess.call(["ray", "stop", "-f"])
        return ""


graph = Receiver.bind(NodeKiller.bind())
