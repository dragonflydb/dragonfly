from dragonfly import dfly_multi_test_args


@dfly_multi_test_args({'keys_output_limit': 512}, {'keys_output_limit': 1024})
class TestKeys:
    def test_max_keys(self, client, df_server):
        max_keys = df_server['keys_output_limit']

        for x in range(max_keys*3):
            client.set(str(x), str(x))
        keys = client.keys()
        assert len(keys) in range(max_keys, max_keys+512)
