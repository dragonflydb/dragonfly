from dragonfly import dfly_multi_test_args


@dfly_multi_test_args(["--keys_output_limit", "512"], ["--keys_output_limit", "1024"])
class TestKeys:
    def test_max_keys(self, client):
        for x in range(8192):
            client.set(str(x), str(x))
        keys = client.keys()
        assert len(keys) in [513, 1025]
