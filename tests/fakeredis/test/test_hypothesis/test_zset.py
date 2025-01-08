from test.test_hypothesis.base import (
    BaseTest,
    commands,
    values,
    keys,
    common_commands,
    counts,
    int_as_bytes,
    fields,
)
import hypothesis.strategies as st


class TestZSet(BaseTest):
    zset_commands = (
        commands(
            st.just("zadd"),
            keys,
            *zero_or_more("nx", "xx", "ch", "incr"),
            st.lists(st.tuples(scores, fields)),
        )
        | commands(st.just("zcard"), keys)
        | commands(st.just("zcount"), keys, score_tests, score_tests)
        | commands(st.just("zincrby"), keys, scores, fields)
        | commands(
            st.sampled_from(["zrange", "zrevrange"]),
            keys,
            counts,
            counts,
            optional("withscores"),
        )
        | commands(
            st.sampled_from(["zrangebyscore", "zrevrangebyscore"]),
            keys,
            score_tests,
            score_tests,
            limits,
            optional("withscores"),
        )
        | commands(st.sampled_from(["zrank", "zrevrank"]), keys, fields)
        | commands(st.just("zrem"), keys, st.lists(fields))
        | commands(st.just("zremrangebyrank"), keys, counts, counts)
        | commands(st.just("zremrangebyscore"), keys, score_tests, score_tests)
        | commands(st.just("zscore"), keys, fields)
        | st.builds(
            build_zstore,
            command=st.sampled_from(["zunionstore", "zinterstore"]),
            dest=keys,
            sources=st.lists(st.tuples(keys, float_as_bytes)),
            weights=st.booleans(),
            aggregate=st.sampled_from([None, "sum", "min", "max"]),
        )
    )
    # TODO: zscan, zpopmin/zpopmax, bzpopmin/bzpopmax, probably more
    create_command_strategy = commands(
        st.just("zadd"), keys, st.lists(st.tuples(scores, fields), min_size=1)
    )
    command_strategy = zset_commands | common_commands


class TestZSetNoScores(BaseTest):
    create_command_strategy = zset_no_score_create_commands
    command_strategy = zset_no_score_commands | common_commands
