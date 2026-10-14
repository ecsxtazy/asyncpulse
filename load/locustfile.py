import uuid

from locust import HttpUser, between, task


class SearchUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task
    def search(self):
        with self.client.post(
            "/search", json={"query": str(uuid.uuid4()), "use_cache": False}, catch_response=True
        ) as response:
            if response.status_code == 200:
                payload = response.json()
                if payload["status"] != "completed" or any(
                    item["error"] for item in payload["results"]
                ):
                    response.failure("Incomplete search")
