import asyncio
import requests
from typing import Dict
from fastapi import FastAPI

from ray import serve


DEFAULT_TARGET_URL = "http://google.com/"

app = FastAPI()


@serve.deployment(
    num_replicas=1,
    route_prefix="/",
    user_config={
        "target_url": DEFAULT_TARGET_URL,
    },
)
@serve.ingress(app)
class Pinger:
    def __init__(self):
        self.target_url = ""
        self.live = False
        self.total_num_requests = 0
        self.total_successful_requests = 0
        self.total_failed_requests = 0
        self.current_num_requests = 0
        self.current_successful_requests = 0
        self.current_failed_requests = 0
        self.failed_responses = dict()

    def reconfigure(self, config: Dict):
        self.stop_requesting()
        new_target_url = config.get("target_url", DEFAULT_TARGET_URL)
        print(f'Changing target URL from "{self.target_url}" to "{new_target_url}"')
        self.target_url = new_target_url

    @app.get("/")
    def root(self):
        return "Hi, I'm a pinger!"

    @app.get("/start")
    async def start_requesting(self):
        if self.live:
            return "Already sending requests."
        else:
            print(f'Starting to send requests to URL "{self.target_url}"')
            self.live = True
            while self.live:
                response = requests.post(self.target_url, json={}, timeout=3)
                if response.status_code == 200:
                    self.count_successful_request()
                else:
                    self.count_failed_request(response.status_code)
                if self.current_num_requests % 3 == 0:
                    print(
                        f"Sent {self.current_num_requests} "
                        f'requests to "{self.target_url}".'
                    )
                await asyncio.sleep(2)

    @app.get("/stop")
    def stop_requesting(self):
        print(f'Stopping requests to URL "{self.target_url}".')
        self.live = False
        self.reset_current_counters()
        return "Stopped."

    @app.get("/info")
    def get_info(self):
        info = {
            "Live": self.live,
            "Target URL": self.target_url,
            "Total number of requests": self.total_num_requests,
            "Total successful requests": self.total_successful_requests,
            "Total failed requests": self.total_failed_requests,
            "Current number of requests": self.current_num_requests,
            "Current successful requests": self.current_successful_requests,
            "Current failed requests": self.current_failed_requests,
            "Failed response status codes": self.failed_responses,
        }
        return info

    def reset_current_counters(self):
        self.current_num_requests = 0
        self.current_successful_requests = 0
        self.current_failed_requests = 0

    def count_successful_request(self):
        self.total_num_requests += 1
        self.total_successful_requests += 1
        self.current_num_requests += 1
        self.current_successful_requests += 1

    def count_failed_request(self, status_code: int):
        self.total_num_requests += 1
        self.total_failed_requests += 1
        self.current_num_requests += 1
        self.current_failed_requests += 1
        self.failed_responses[status_code] = (
            self.failed_responses.get(status_code, 0) + 1
        )


graph = Pinger.bind()
