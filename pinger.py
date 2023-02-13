import time
import asyncio
import requests
from typing import Dict
from fastapi import FastAPI

from constants import RECEIVER_KILL_KEY, KillOptions

from ray import serve
from ray.util.metrics import Counter, Gauge


DEFAULT_BEARER_TOKEN = "default"
DEFAULT_TARGET_URL = "http://google.com/"
DEFAULT_KILL_INTERVAL = 1000000

app = FastAPI()


@serve.deployment(
    num_replicas=1,
    route_prefix="/",
    user_config={
        "target_url": DEFAULT_TARGET_URL,
        "bearer_token": DEFAULT_BEARER_TOKEN,
        "kill_interval": DEFAULT_KILL_INTERVAL,
    },
)
@serve.ingress(app)
class Pinger:
    def __init__(self):
        self.kill_interval = -1
        self.target_url = ""
        self.bearer_token = ""
        self.live = False
        self.total_num_requests = 0
        self.total_successful_requests = 0
        self.total_failed_requests = 0
        self.total_kill_requests = 0
        self.current_num_requests = 0
        self.current_successful_requests = 0
        self.current_failed_requests = 0
        self.current_kill_requests = 0
        self.failed_response_counts = dict()
        self.failed_response_reasons = dict()

        self.request_counter = Counter(
            "pinger_num_requests",
            description="Number of requests.",
            tag_keys=("class",),
        )
        self.request_counter.set_default_tags({"class": "Pinger"})

        self.success_counter = Counter(
            "pinger_num_requests_succeeded",
            description="Number of successful requests.",
            tag_keys=("class",),
        )
        self.success_counter.set_default_tags({"class": "Pinger"})

        self.fail_counter = Counter(
            "pinger_num_requests_failed",
            description="Number of failed requests.",
            tag_keys=("class",),
        )
        self.fail_counter.set_default_tags({"class": "Pinger"})

        self.latency_gauge = Gauge(
            "pinger_request_latency",
            description="Latency of last successful request.",
            tag_keys=("class",),
        )
        self.latency_gauge.set_default_tags({"class": "Pinger"})

    def reconfigure(self, config: Dict):
        self.stop_requesting()

        new_kill_interval = config.get("kill_interval", DEFAULT_KILL_INTERVAL)
        print(
            f"Changing kill interval from {self.kill_interval} to {new_kill_interval}."
        )
        self.kill_interval = new_kill_interval

        new_target_url = config.get("target_url", DEFAULT_TARGET_URL)
        print(f'Changing target URL from "{self.target_url}" to "{new_target_url}"')
        self.target_url = new_target_url

        new_bearer_token = config.get("bearer_token", DEFAULT_BEARER_TOKEN)
        print(
            f'Changing bearer token from "{self.bearer_token}" to "{new_bearer_token}"'
        )
        self.bearer_token = new_bearer_token

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
                json_payload = {RECEIVER_KILL_KEY: KillOptions.SPARE}
                if self.send_kill_request():
                    print("Sending kill request.")
                    json_payload = {RECEIVER_KILL_KEY: KillOptions.KILL}

                start_time = time.time()
                try:
                    response = requests.post(
                        self.target_url,
                        headers={"Authorization": f"Bearer {self.bearer_token}"},
                        json=json_payload,
                        timeout=3,
                    )
                    latency = time.time() - start_time

                    self.request_counter.inc()
                    if self.send_kill_request():
                        self.count_successful_request(kill_request=True)
                        self.success_counter.inc()
                    elif response.status_code == 200:
                        self.count_successful_request()
                        self.latency_gauge.set(latency)
                        self.success_counter.inc()
                    else:
                        self.count_failed_request(
                            response.status_code, reason=response.text
                        )
                        self.fail_counter.inc()
                    if self.current_num_requests % 3 == 0:
                        print(
                            f"{time.strftime('%b %d – %l:%M%p: ')}"
                            f"Sent {self.current_num_requests} "
                            f'requests to "{self.target_url}".'
                        )
                except Exception as e:
                    self.count_failed_request(-1, reason=repr(e))
                    self.fail_counter.inc()
                    print(
                        f"{time.strftime('%b %d – %l:%M%p: ')}"
                        f"Got exception: \n{repr(e)}"
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
            "Total kill requests": self.total_kill_requests,
            "Current number of requests": self.current_num_requests,
            "Current successful requests": self.current_successful_requests,
            "Current failed requests": self.current_failed_requests,
            "Current kill requests": self.current_kill_requests,
            "Failed response counts": self.failed_response_counts,
            "Failed response reasons": self.failed_response_reasons,
        }
        return info

    def send_kill_request(self) -> bool:
        """Returns whether or not to send a kill request."""

        return (
            self.kill_interval > 0
            and self.current_num_requests % self.kill_interval == self.kill_interval - 1
        )

    def reset_current_counters(self):
        self.current_num_requests = 0
        self.current_successful_requests = 0
        self.current_failed_requests = 0
        self.current_kill_requests = 0

    def count_successful_request(self, kill_request: bool = False):
        self.total_num_requests += 1
        self.total_successful_requests += 1
        self.current_num_requests += 1
        self.current_successful_requests += 1
        if kill_request:
            self.total_kill_requests += 1
            self.current_kill_requests += 1

    def count_failed_request(self, status_code: int, reason: str = ""):
        self.total_num_requests += 1
        self.total_failed_requests += 1
        self.current_num_requests += 1
        self.current_failed_requests += 1
        self.failed_response_counts[status_code] = (
            self.failed_response_counts.get(status_code, 0) + 1
        )
        if status_code in self.failed_response_reasons:
            self.failed_response_reasons[status_code].add(reason)
        else:
            self.failed_response_reasons[status_code] = set(reason)


graph = Pinger.bind()
