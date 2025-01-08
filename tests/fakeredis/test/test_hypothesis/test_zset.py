import operator

import hypothesis.strategies as st

from test.test_hypothesis.base import (
    BaseTest,
    commands,
    keys,
    common_commands,
    counts,
    fields,
    zero_or_more,
    scores,
    Command,
    float_as_bytes,
)

score_tests = scores | st.builds(lambda x: b"(" + repr(x).encode(), scores)
limits = st.just(()) | st.tuples(st.just("limit"), counts, counts)
string_tests = st.sampled_from([b"+", b"-"]) | st.builds(
    operator.add, st.sampled_from([b"(", b"["]), fields
)
zset_no_score_create_commands = commands(
    st.just("zadd"), keys, st.lists(st.tuples(st.just(0), fields), min_size=1)
)
zset_no_score_commands = (  # TODO: test incr
    commands(
        st.just("zadd"),
        keys,
        *zero_or_more("nx", "xx", "ch", "incr"),
        st.lists(st.tuples(st.just(0), fields)),
    )
    | commands(st.just("zlexcount"), keys, string_tests, string_tests)
    | commands(
        st.sampled_from(["zrangebylex", "zrevrangebylex"]),
        keys,
        string_tests,
        string_tests,
        limits,
    )
    | commands(st.just("zremrangebylex"), keys, string_tests, string_tests)
)


def optional(arg):
    return st.none() | st.just(arg)


def build_zstore(command, dest, sources, weights, aggregate) -> Command:
    args = [command, dest, len(sources)]
    args += [source[0] for source in sources]
    if weights:
        args.append("weights")
        args += [source[1] for source in sources]
    if aggregate:
        args += ["aggregate", aggregate]
    return Command(args)


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
