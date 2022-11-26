from . import dfly_multi_test_args
from .utility import batch_fill_data, gen_test_data


@dfly_multi_test_args({'keys_output_limit': 512}, {'keys_output_limit': 1024})
class TestKeys:
    def test_max_keys(self, client, df_server):
        max_keys = df_server['keys_output_limit']

        batch_fill_data(client, gen_test_data(max_keys * 3))

        keys = client.keys()
        assert len(keys) in range(max_keys, max_keys+512)
