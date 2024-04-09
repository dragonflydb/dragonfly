# Dragonfly Frequently Asked Questions

- [Dragonfly Frequently Asked Questions](#dragonfly-frequently-asked-questions)
  - [What is the license model of Dragonfly? Is it an open source?](#what-is-the-license-model-of-dragonfly-is-it-an-open-source)
  - [Can I use dragonfly in production?](#can-i-use-dragonfly-in-production)
  - [We benchmarked Dragonfly and we have not reached 4M qps throughput as you advertised.](#we-benchmarked-dragonfly-and-we-have-not-reached-4m-qps-throughput-as-you-advertised)
  - [Dragonfly provides vertical scale, but we can achieve similar throughput with X nodes in a Redis cluster.](#dragonfly-provides-vertical-scale-but-we-can-achieve-similar-throughput-with-x-nodes-in-a-redis-cluster)
  - [If only Dragonfly had this command I would use it for sure](#if-only-dragonfly-had-this-command-i-would-use-it-for-sure)


## What is the license model of Dragonfly? Is it an open source?
Dragonfly is released under [BSL 1.1](../LICENSE.md) (Business Source License).
BSL 1.1 is considered to be "source available" license and it's not strictly open-source license.
We believe that a [BSL 1.1](https://spdx.org/licenses/BUSL-1.1.html) license is more permissive
than licenses like AGPL, and it will allow us to
provide a competitive commercial service using our technology. In general terms,
it means that Dragonfly's code is free to use and free to change as long as you do not sell services directly related to
Dragonfly or in-memory datastores.
We followed the trend of other technological companies like Elastic, Redis, MongoDB, Cockroach labs,
Redpanda Data to protect our rights to provide service and support for the software we are building.

## Can I use dragonfly in production?
License wise you are free to use dragonfly in your production as long as you do not provide Dragonfly as a managed service.
From a code maturity point of view, Dragonfly's code is covered with unit testing and the regression tests.
However as with any new software there are use cases that are hard to test and predict.
We advise you to run your own particular use case on dragonfly for a few days before considering production usage.

## We benchmarked Dragonfly and we have not reached 4M qps throughput as you advertised.
We conducted our experiments using a load-test generator called `memtier_benchmark`,
and we run benchmarks on AWS network-enhanced instance `c6gn.16xlarge` on recent Linux kernel versions.
Dragonfly might reach smaller throughput on other instances, but we would
still expect to reach around 1M+ qps on instances with 16-32 vCPUs.

## Dragonfly provides vertical scale, but we can achieve similar throughput with X nodes in a Redis cluster.
Dragonfly optimizes the use of underlying hardware, allowing it to run efficiently on instances as small as 8GB,
 and scale vertically to large 2TB machines with 128 cores. This versatility significantly
 reduces the complexity of running cluster workloads on a single node, saving hardware resources and costs.
 More importantly, it diminishes the total cost
 of ownership associated with managing multi-node clusters. In contrast, Redis in cluster
 mode imposes limitations on multi-key and transactional operations, whereas Dragonfly maintains
 the same semantics as a single-node Redis system.
 Furthermore, scaling out horizontally with small instances can lead to instability
 in production environments.
 We believe that large-scale deployments of in-memory stores require both vertical and horizontal scaling,
 which is not efficiently achievable with an in-memory store like Redis.

## If only Dragonfly had this command I would use it for sure
Dragonfly implements ~190 Redis commands which we think represent a good coverage of the market.
However this is not based empirical data. Having said that, if you have commands that are not covered,
please feel free to open an issue for that or vote for an existing issue.
We will do our best to prioritise those commands according to their popularity.
