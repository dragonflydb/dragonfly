# Dragonfly Frequently Asked Questions

- [What is the license model of Dragonfly? Is it an open source?](#what-is-the-license-model-of-dragonfly-is-it-an-open-source)
- [Can I use dragonfly in production?](#can-i-use-dragonfly-in-production)
- [Dragonfly provides vertical scale, but we can achieve similar throughput with X nodes in a Redis cluster.](#dragonfly-provides-vertical-scale-but-we-can-achieve-similar-throughput-with-x-nodes-in-a-redis-cluster.)
- [What if I have some commands that are missing?](#what_if_i_have_some_commands_that_are_missing)


## What is the license model of Dragonfly? Is it an open source?
Dragonfly is released under [BSL 1.1](../LICENSE.md) (Business Source License). We believe that a BSL license is more permissive than AGPL-like licenses. In general terms, it means that dragonfly's code is free to use and free to change as long as you do not sell services directly related to dragonfly or in-memory datastores.
We followed the trend of other technological companies like Elastic, Redis, MongoDB, Cockroach labs, Redpanda Data to protect our rights to provide service and support for the software we are building.

## Can I use dragonfly in production?
License wise you are free to use dragonfly in your production as long as you do not provide dragonfly as a managed service. From a code maturity point of view, Dragonfly's code is covered with unit testing. However as with any new software there are use cases that are hard to test and predict. We advise you to run your own particular use case on dragonfly for a few days before considering production usage.

## Dragonfly provides vertical scale, but we can achieve similar throughput with X nodes in a Redis cluster.
Dragonfly utilizes the underlying hardware in an optimal way. Meaning it can run on small 8GB instances and scale vertically to large 768GB machines with 64 cores. This versatility allows to drastically reduce complexity of running cluster workloads to a single node saving hardware resources and costs. More importantly, it reduces the complexity (total cost of ownership) of handling the multi-node cluster. In addition, Redis cluster-mode imposes some limitations on multi-key and transactional operations while Dragonfly provides the same semantics as single node Redis.

## If only Dragonfly had this command I would use it for sure
Dragonfly implements ~130 Redis commands which we think represent a good coverage of the market. However this is not based empirical data. Having said that, if you have commands that are not covered, please feel free to open an issue for that or vote for an existing issue. We will do our best to prioritise those commands according to their popularity.
