#!/usr/bin/env python3
import os
from typing import List

import attr
from kubernetes import client, config, watch
from kubernetes.client import models


@attr.define
class TestCase:
    name: str
    timeout: int = 6000


TEST_CASES: List[TestCase] = [TestCase(name="default")]

config.load_kube_config()
v1 = client.BatchV1Api()


def wait_for_job_completion(namespace: str, job_name: str, timeout_seconds: int):
    w = watch.Watch()
    try:
        for event in w.stream(
            v1.list_namespaced_job, namespace=namespace, timeout_seconds=timeout_seconds
        ):
            job = event["object"]  # type: ignore

            if job.metadata.name != job_name:  # type: ignore
                continue

            conditions = job.status.conditions  # type: ignore
            if not conditions:
                continue

            for condition in conditions:
                if condition.status != "True":
                    continue

                if condition.type == "Complete":
                    print(f"Job {job_name} completed successfully.")
                    return

                if condition.type == "Failed":
                    print(f"Job {job_name} failed.")
                    return
    finally:
        w.stop()

    print(
        f"Timeout or job {job_name} did not reach a complete/failed state within the specified timeout."
    )


def main():
    namespace = os.environ["NAMESPACE"]

    for case in TEST_CASES:
        job_name = f"memtier-benchmark-{case.name}"

        print(f"Running case {job_name}")
        v1.create_namespaced_job(
            namespace=namespace,
            body=models.V1Job(
                metadata={"name": f"memtier-benchmark-{case.name}"},
                spec=models.V1JobSpec(
                    backoff_limit=0,
                    template=models.V1JobTemplateSpec(
                        spec={
                            "restartPolicy": "Never",
                            "containers": [
                                models.V1Container(
                                    name="memtier",
                                    image="redislabs/memtier_benchmark:latest",
                                    args=[
                                        "memtier_benchmark --pipeline=30 --key-maximum=10000 -c 10 -t 2 --requests=500000 --expiry-range=10-100 --reconnect-interval=10000 --distinct-client-seed --hide-histogram -s dragonfly-sample"
                                    ],
                                    command=[
                                        # This is important! without it memtier cannot DIG the dragonfly SVC domain
                                        "sh",
                                        "-c",
                                    ],
                                    resources={
                                        "requests": {"cpu": "2", "memory": "500Mi"},
                                        "limits": {"cpu": "2", "memory": "500Mi"},
                                    },
                                )
                            ],
                        }
                    ),
                ),
            ),
        )
        wait_for_job_completion(namespace, job_name, case.timeout)


if __name__ == "__main__":
    main()
