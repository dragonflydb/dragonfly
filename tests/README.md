# System tests


## Pytest

The tests assume you have the "dragonfly" binary in `<root>/build-dbg` directory.
You can override the location of the binary using `DRAGONFLY_HOME` environment var.

to run pytest, run:
`pytest -xv pytest`


# Integration tests
To simplify running integration test each package should have its own Dockerfile. The Dockerfile should contain everything needed in order to test the package against Drafongly. Docker can assume Dragonfly is running on localhost:6379.
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
