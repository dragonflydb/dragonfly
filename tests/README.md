# System tests


## Pytest

The tests assume you have the "dragonfly" binary in `<root>/build-dbg` directory.
You can override the location of the binary using `DRAGONFLY_PATH` environment var.

### Important fixtures

- `df_server` is the default instance that is available for testing. Use the `dfly_args` decorator to change its default arguments.
- `client` and `async_client` are clients to the default instance. The default instance is re-used accross tests with the same arguments, but each new client flushes the instance.
- `pool` and `async_pool` are client pools that are connected to the default instance

### Custom arguments

- use `--gdb` to start all instances inside gdb.
- use `--df arg=val` to pass custom arguments to all dragonfly instances. Can be used multiple times.
- use `--log-seeder file` to store all single-db commands from the lastest tests seeder inside file.
- use `--existing-port` to use an existing instance for tests instead of starting one
- use `--rand-seed` to set the global random seed. Makes the seeder predictable.
- use `--repeat <N>` to run a test multiple times.

for example,

```sh
pytest dragonfly/connection_test.py -s  --df logtostdout --df vmodule=dragonfly_connection=2 -k test_subscribe
```
### Before you start
Please make sure that you have python 3 installed on you local host.
If have more both python 2 and python 3 installed on you host, you can run the tests with the following command:
```
python3 -m pytest -xv dragonfly
```
It is advisable to use you python virtual environment: [python virtual environment](https://docs.python.org/3/library/venv.html).
To activate it, run:
```
source <virtual env name>/bin/activate
```
Then install all the required dependencies for the tests:
```
pip3 install -r dragonfly/requirements.txt
```

### Running the tests
to run pytest, run:
`pytest -xv dragonfly`

to run selectively, use:
`pytest -xv dragonfly -k <substring>`
For more pytest flags [check here](https://fig.io/manual/pytest).

## Writing tests
The [Getting Started](https://docs.pytest.org/en/7.1.x/getting-started.html) guide is a great resource to become familiar with writing pytest test cases.

Pytest will recursively search the `tests/dragonfly` directory for files matching the patterns `test_*.py` or `*_test.py` for functions matching these [rules](https://docs.pytest.org/en/7.1.x/explanation/goodpractices.html#conventions-for-python-test-discovery):
- Functions or methods outside of a class prefixed by `test`
- Functions or methods prefixed by `test` inside a class prefixed by `Test` (without an `__init__` method)

**Note**: When making a new directory in `tests/dragonfly` be sure to create an `__init__.py` file to avoid [name conflicts](https://docs.pytest.org/en/7.1.x/explanation/goodpractices.html#tests-outside-application-code)

### Passing CLI commands to Dragonfly
To pass custom flags to the Dragonfly executable two class decorators have been created. `@dfly_args` allows you to pass a list of parameters to the Dragonfly executable, similarly `@dfly_multi_test_args` allows you to specify multiple parameter configurations to test with a given test class.

In the case of `@dfly_multi_test_args` each parameter configuration will create one Dragonfly instance which each test will receive a client to as described in the [above section](#interacting-with-dragonfly)

Parameters can use environmental variables with a formatted string where `"{<VAR>}"` will be replaced with the value of the `<VAR>` environment variable. Due to [current pytest limtations](https://github.com/pytest-dev/pytest/issues/349) fixtures cannot be passed to either of these decorators, this is currently the provided way to pass the temporary directory path in a CLI parameter.

### Test Examples
- **[snapshot_test](./dragonfly/snapshot_test.py)**: Example test using `@dfly_args`, environment variables and pre-test setup
- **[generic_test](./dragonfly/generic_test.py)**: Example test using `@dfly_multi_test_args`
- **[connection_test](./dragonfly/connection_test.py)**: Example for testing running with multiple asynchronous connections.

### Writing your own fixtures
The fixture functions located in [conftest.py](./dragonfly/conftest.py).
You can write your own fixture inside this file, as seem fit. Just make sure, before adding new fixture that there maybe one already written.
Try to make the fixture running at the smallest scope possible to ensure that the test can be independent of each other (this will ensure no side effect - match our policy of "share nothing").

### Managing test environment
Do forget to add any new dependency that you may created to [dragonfly/requirement.txt](./dragonfly/requirements.txt) file.
You can do so by running
```
pip3 freeze > requirements.txt
```
from [dragonfly](./dragonfly/) directory.

# Integration tests
Integration tests are located in the `integration` folder.

To simplify running integration test each package should have its own Dockerfile. The Dockerfile should contain everything needed in order to test the package against Dragonfly. Docker can assume Dragonfly is running on localhost:6379.
To run the test:
```
docker build -t [test-name] -f [test-dockerfile-name] .
docker run --network=host [test-name]
```

## Node-Redis
Integration test for node-redis client.
Build:
```
docker build -t node-redis-test -f ./node-redis.Dockerfile .
```
Run:
```
docker run --network=host node-redis-test
```

to run only selected tests use:

```
docker run --network=host node-redis-test npm run test -w ./packages/client -- --redis-version=2.8 -g <regex>
```

In general, you can add this way any option from [mocha framework](https://mochajs.org/#command-line-usage).

## ioredis
NOTE: we are depending on some changes to ioredis test, in order to pass more tests, as we are currently failing
because in monitor command we always returning the command name in upper case, and the tests expected it to
be in lower case.

Integration tests for ioredis client.
[ioredis](https://github.com/luin/ioredis) is a robust, performance-focused and full-featured Redis client for Node.js.
It contains a very extensive test coverage for Redis. Currently not all features are supported by Dragonfly.
As such please use the scripts for running the test successfully -
 **[run_ioredis_on_docker.sh](./integration/run_ioredis_on_docker.sh)**: to run the supported tests on a docker image
 Please note that you can run this script in two forms:

 If the image is already build:
 ```
 ./integration/run_ioredis_on_docker.sh
 ```

A more safe way is to build the image (or ensure that it is up to date), and then execute the tests:
```
 ./integration/run_ioredis_on_docker.sh --build
 ```
 The the "--build" first build the image and then execute the tests.
 Please do not try to run out of docker image as this brings the correct version and patch some tests.
Please note that the script only run tests that are currently supported
You can just build the image with

Build:
```
docker build -t ioredis-test -f ./ioredis.Dockerfile .
```

For more details on the entrypoint setup, compare the `ioredis.Dockerfile`
with the npm test script located on the `package.json` of the ioredis project.

## Jedis
Integration test for the Jedis client.
Build:
```
docker build -t jedis-test -f ./jedis.Dockerfile .
```
Run:
```
docker run --network=host jedis-test
```
