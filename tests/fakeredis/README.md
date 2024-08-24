Running FakeRedis tests on Dragonfly
====================================

FakeRedis is a Python library that provides a full implementation of the Redis protocol. It is useful for testing Redis
clients and for running Redis commands in Python code without having a running Redis server.

The tests in this directory are running against FakeRedis and against a dragonfly instance.
The results are then compared to ensure that the two implementations are consistent.

## Prerequisites

- Python 3.10 or above is required to run the tests.
- Poetry is required to install the dependencies.
- A dragonfly instance running on port 6380.

## Setup environment

1. Install Poetry by following the instructions at https://python-poetry.org/docs/#installation.
2. From the root directory of the tests (`dragonfly/tests/fakeredis`) run `poetry env use python3.10` (or higher) to
   create a virtual environment for Python 3.10.
3. Run `poetry install` to install the dependencies.
4. Run `poetry run pytest -v` to run all the tests.
5. Or alternatively, run `poetry run pytest -v test/{test-name}` to run a specific set of tests.

## Tests

- `test_connection.py`: Tests for the connection parameters to the Dragonfly server.
- `test_zadd.py`: Considering the various options for the ZADD command, it has its own set of tests.
- `test_json/*.py`: Tests for the JSON commands.
- `test_stack/*.py`: Tests for the stack commands, bloom filter, cuckoo filter, CMS, TDigest, time-series, top-k.
- `test_mixins/*.py`: Tests for various generic commands: bitmap, geospacial, hash, list, pubsub, scripting, streams,
  string, etc.
- `test_hypothesis.py`: Hypothesis tests for the mixins commands. These tests are using [hypothesis][1] and generate
  random tests with edge cases. Note these tests take significantly more time to run.

## General info

- `@pytest.mark.unsupported_server_types("dragonfly")` decorator indicates to pytest that the test should not run on
  dragonfly.
  - Some tests are skipped the commands are CURRENTLY not supported (e.g., `GEORARIUS`).
  - Others are skipped because they cause an expected behavior, and usually marked with TODO comment as well.

[1]: https://hypothesis.readthedocs.io/en/latest/
