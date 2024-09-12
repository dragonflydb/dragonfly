<p align="center">
  <a href="https://dragonflydb.io">
    <img  src="/.github/images/logo-full.svg"
      width="284" border="0" alt="Dragonfly">
  </a>
</p>



[![ci-tests](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml/badge.svg)](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml) [![Twitter URL](https://img.shields.io/twitter/follow/dragonflydbio?style=social)](https://twitter.com/dragonflydbio)

> 在您继续之前，请考虑给我们一个 GitHub 星标 ⭐️。谢谢！

其他语言:  [English](README.md) [日本語](README.ja-JP.md) [한국어](README.ko-KR.md)

[主页](https://dragonflydb.io/) • [快速入门](https://github.com/dragonflydb/dragonfly/tree/main/docs/quick-start) • [社区 Discord](https://discord.gg/HsPjXGVH85) • [Dragonfly 论坛](https://dragonfly.discourse.group/) • [加入 Dragonfly 社区](https://www.dragonflydb.io/community)

[GitHub Discussions](https://github.com/dragonflydb/dragonfly/discussions) • [GitHub Issues](https://github.com/dragonflydb/dragonfly/issues) • [贡献指南](https://github.com/dragonflydb/dragonfly/blob/main/CONTRIBUTING.md)

## 全世界最快的内存数据库

Dragonfly是一种针对现代应用程序负荷需求而构建的内存数据库，完全兼容Redis和Memcached的 API，迁移时无需修改任何代码。相比于这些传统的内存数据库，Dragonfly提供了其25倍的吞吐量，高缓存命中率和低尾延迟，并且对于相同大小的工作负载运行资源最多可减少80%。

## 目录

- [基准测试](#基准测试)
- [快速入门](https://github.com/dragonflydb/dragonfly/tree/main/docs/quick-start)
- [配置方法](#配置方法)
- [开发路线和开发现状](#开发路线和开发现状)
- [设计决策](#设计决策)
- [开发背景](#开发背景)

## <a name="基准测试"><a/> 基准测试

<img src="http://static.dragonflydb.io/repo-assets/aws-throughput.svg" width="80%" border="0"/>

Dragonfly在c6gn.16xlarge上达到了每秒380万个查询（QPS），相比于Redis，吞吐量提高了25倍。

在Dragonfly的峰值吞吐量下，P99延迟如下：

| op    | r6g   | c6gn  | c7g   |
| ----- | ----- | ----- | ----- |
| set   | 0.8ms | 1ms   | 1ms   |
| get   | 0.9ms | 0.9ms | 0.8ms |
| setex | 0.9ms | 1.1ms | 1.3ms |

*所有基准测试均使用`memtier_benchmark`（见下文），根据服务器类型和实例类型调整线程数。`memtier`运行在独立的c6gn.16xlarge机器上。对于setex基准测试，我们使用了500的到期范围，以便其能够存活直到测试结束。*

```bash
  memtier_benchmark --ratio ... -t <threads> -c 30 -n 200000 --distinct-client-seed -d 256 \
     --expiry-range=...
```

当以管道模式运行，并设置参数`--pipeline=30`时，Dragonfly可以实现**10M qps**的SET操作和 **15M qps**的GET操作。

### Memcached / Dragonfly

我们在 AWS 的 `c6gn.16xlarge` 实例上比较了 memcached 和 Dragonfly。如下图所示，与 memcached 相比，Dragonfly 的吞吐量在读写两方面上都占据了优势，并且在延迟方面也还不错。对于写入工作，Dragonfly 的延迟更低，这是由于在 memcached 的写入路径上存在竞争（请参见[此处](docs/memcached_benchmark.md)）。

#### SET benchmark

|  Server   | QPS(thousands qps) | latency 99% |  99.9%  |
| :-------: | :----------------: | :---------: | :-----: |
| Dragonfly |       🟩 3844       |   🟩 0.9ms   | 🟩 2.4ms |
| Memcached |        806         |    1.6ms    |  3.2ms  |

#### GET benchmark

| Server    | QPS(thousands qps) | latency 99% |  99.9%  |
| --------- | :----------------: | :---------: | :-----: |
| Dragonfly |       🟩 3717       |     1ms     |  2.4ms  |
| Memcached |        2100        |  🟩 0.34ms   | 🟩 0.6ms |


对于读取基准测试，Memcached 表现出了更低的延迟，但在吞吐量方面比不上Dragonfly。

### 内存效率

在接下来的测试中，我们使用 `debug populate 5000000 key 1024` 命令向 Dragonfly 和 Redis 分别写入了约 5GB 的数据。然后我们使用 `memtier` 发送更新流量并使用 `bgsave` 命令启动快照。下图清楚地展示了这两个服务器在内存效率方面的表现。

<img src="http://static.dragonflydb.io/repo-assets/bgsave-memusage.svg" width="70%" border="0"/>

在空闲状态下，Dragonfly 比 Redis 节省约 30% 的内存。
在快照阶段，Dragonfly 也没有显示出任何明显的内存增加。
但同时，Redis 在峰值时的内存几乎达到了 Dragonfly 的 3 倍。
Dragonfly 完成快照也很快，仅在启动后几秒钟内就完成了。
有关 Dragonfly 内存效率的更多信息，请参见 [dashtable 文档](/docs/dashtable.md)。



## <a name="开发路线和开发现状"><a/>配置方法

Dragonfly 支持 Redis 的常见参数。
例如，您可以运行：`dragonfly --requirepass=foo --bind localhost`。

目前，Dragonfly 支持以下 Redis 特定参数：

* `port`：Redis 连接端口，默认为 `6379`。
* `bind`：使用本地主机名仅允许本地连接，使用公共 IP 地址允许外部连接到**该 IP 地址**。
* `requirepass`：AUTH 认证密码，默认为空 `""`。
* `maxmemory`：限制数据库使用的最大内存（以字节为单位）。`0` 表示程序将自动确定其最大内存使用量。默认为 `0`。
* `dir`：默认情况下，dragonfly docker 使用 `/data` 文件夹进行快照。CLI 使用的是 `""`。你可以使用 `-v` docker 选项将其映射到主机文件夹。
* `dbfilename`：保存/加载数据库的文件名。默认为 `dump`；

此外，还有 Dragonfly 特定的参数选项：

* `memcached_port`：在此端口上启用 memcached 兼容的 API。默认禁用。

* `keys_output_limit`：在`keys` 命令中返回的最大键数。默认为 `8192`。

  `keys` 命令是危险命令。我们会截断结果以避免在获取太多键时内存溢出。

* `dbnum`：`select` 支持的最大数据库数。

* `cache_mode`：请参见下面的 [缓存](#全新的缓存设计) 部分。

* `hz`：键到期评估频率。默认为 `100`。空闲时，使用较低的频率可以占用较少的 CPU资源，但这会导致清理过期键的速度下降。

* `snapshot_cron`：定时自动备份快照的 cron 表达式，使用标准的、精确到分钟的 cron 语法。默认为空 `""`。

  下面是一些 cron 表达式的示例，更多关于此参数的细节请参见[文档](https://www.dragonflydb.io/docs/managing-dragonfly/backups#the-snapshot_cron-flag)。

  | Cron 表达式      | 描述                               |
  |---------------|----------------------------------|
  | `* * * * *`   | 每分钟                              |
  | `*/5 * * * *` | 每隔 5 分钟 (00:00, 00:05, 00:10...) |
  | `5 */2 * * *` | 每隔 2 小时的第 5 分钟                   |
  | `0 0 * * *`   | 每天的 00:00 午夜                     |
  | `0 6 * * 1-5` | 从星期一到星期五的每天 06:00 黎明             |
* `primary_port_http_enabled`：如果为 true，则允许在主 TCP 端口上访问 HTTP 控制台。默认为 `true`。

* `admin_port`：如果设置，将在指定的端口上启用对控制台的管理访问。支持 HTTP 和 RESP 协议。默认禁用。

* `admin_bind`：如果设置，将管理控制台 TCP 连接绑定到给定地址。支持 HTTP 和 RESP 协议。默认为 `any`。

* `admin_nopass`: 如果设置，允许在不提供任何认证令牌的情况下，通过指定的端口访问管理控制台。同时支持 HTTP 和 RESP 协议。 默认为 `false`。

* `cluster_mode`：支持集群模式。目前仅支持 `emulated`。默认为空 `""`。

* `cluster_announce_ip`：集群模式下向客户端公开的 IP。

### 启动脚本示例，包含常用选项：

```bash
./dragonfly-x86_64 --logtostderr --requirepass=youshallnotpass --cache_mode=true -dbnum 1 --bind localhost --port 6379 --maxmemory=12gb --keys_output_limit=12288 --dbfilename dump.rdb
```
还可以通过运行 `dragonfly --flagfile <filename>` 从配置文件中获取参数，配置文件的每行应该列出一个参数，并用等号代替键值参数的空格。

要获取更多选项，如日志管理或TLS支持，请运行 `dragonfly --help`。

## <a name="开发路线和开发现状"><a/>开发路线和开发现状

目前，Dragonfly支持约185个Redis命令以及除 `cas` 之外的所有 Memcached 命令。
我们几乎达到了Redis 5 API的水平。我们的下一个里程碑更新将会稳定基本功能并实现复刻API。
如果您发现您需要的命令尚未实现，请提出一个Issue。

对于dragonfly-native复制技术，我们正在设计一种分布式日志格式，该格式将支持更高的速度。

在实现复制功能之后，我们将继续实现API 3-6中其他缺失的Redis命令。

请参见[命令参考](https://dragonflydb.io/docs/category/command-reference)以了解Dragonfly当前支持的命令。

## <a name="设计决策"><a/> 设计决策

### 全新的缓存设计

Dragonfly采用单一的自适应缓存算法，该算法非常简单且具备高内存效率。
你可以通过使用 `--cache_mode=true` 参数来启用缓存模式。一旦启用了此模式，Dragonfly将会删除最低概率可能被使用的内容，但这只会在接近最大内存限制时发生。

### 相对准确的过期期限

过期范围限制最高为约8年。此外，**对于大于2^28ms的到期期限**，毫秒精度级别（PEXPIRE/PSETEX等）会被简化到秒级。
这种舍入的误差小于0.001％，我希望这在长时间范围情况下是可以接受的。
如果这不符合你的使用需求，请与我联系或提出一个Issue，并解释您的情况。

关于与Redis实现之间的更多差异，请参见[此处](docs/differences.md)。

### 原生HTTP控制台和兼容Prometheus的标准

默认情况下，Dragonfly允许通过其主TCP端口（6379）进行HTTP访问。没错，您可以通过Redis协议或HTTP协议连接到Dragonfly - 服务器会在连接初始化期间自动识别协议。 不妨在你自己的浏览器中尝试一下。现在HTTP访问没有太多信息可供参考，但在将来，我们计划添加有用的调试和管理信息。如果您转到`: 6379/metrics` URL，您将看到一些兼容Prometheus的标准。

Prometheus导出的标准与Grafana仪表盘兼容，[请参见此处](tools/local/monitoring/grafana/provisioning/dashboards/dashboard.json)。

重要！HTTP控制台仅应在安全网络内访问。如果您将Dragonfly的TCP端口暴露在外部，则建议使用`--http_admin_console=false`或`--nohttp_admin_console`禁用控制台。


## <a name="开发背景"><a/>开发背景

Dragonfly始于一项实验，旨在探索如果在2022年重新设计内存数据库，它会是什么样子。基于我们作为内存存储的用户以及作为云服务公司的工程师的经验教训，我们得知需要保留Dragonfly的两个关键属性：a) 为其所有操作提供原子性保证，b) 保证在非常高的吞吐量下实现低于毫秒的延迟。

我们面临的首要挑战是如何充分利用当今云服务器的CPU、内存和I/O资源。为了解决这个问题，我们使用了 [无共享式架构（shared-nothing architecture）](https://en.wikipedia.org/wiki/Shared-nothing_architecture)，它允许我们在不同的线程之间分割内存存储的空间，使得每个线程可以管理自己的字典数据切片。我们称这些切片为“分片（shards）”。为无共享式架构提供线程和I/O管理功能的库在[这里](https://github.com/romange/helio)开源。

为了提供对多键并发操作的原子性保证，我们使用了最近学术研究的进展。我们选择了论文 ["VLL: a lock manager redesign for main memory database systems”](https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf) 来开发Dragonfly的事务框架。无共享式架构和VLL的选择使我们能够在不使用互斥锁或自旋锁的情况下组合原子的多键操作。这是我们 PoC 的一个重要里程碑，它的性能在商业和开源解决方案中脱颖而出。

我们面临的第二个挑战是为新存储设计更高效的数据结构。为了实现这个目标，我们基于论文["Dash: Scalable Hashing on Persistent Memory"](https://arxiv.org/pdf/2003.07302.pdf)构建了核心哈希表结构。这篇论文本身是以持久性内存为中心的，与主存没有直接相关性。

然而，它非常适用于我们的问题。它提出了一种哈希表设计，允许我们维护Redis字典中存在的两个特殊属性：a) 数据存储增长时的渐进式哈希能力；b）使用无状态扫描操作时，遍历变化的字典的能力。除了这两个属性之外，Dash在CPU和内存方面都更加高效。通过利用Dash的设计，我们能够进一步创新，实现以下功能：

- 针对TTL的高效记录过期功能。
- 一种新颖的缓存驱逐算法，具有比其他缓存策略（如LRU和LFU）更高的命中率，同时**零内存开销**。
- 一种新颖的无fork快照算法。

在我们为Dragonfly打下基础并满意其[性能](#基准测试)后，我们开始实现Redis和Memcached功能。
目前，我们已经实现了约185个Redis命令（大致相当于Redis 5.0 API）和13个Memcached命令。

最后，<br>
<em>我们的使命是构建一个设计良好、超高速、成本效益高的云工作负载内存数据存储系统，利用最新的硬件技术。我们旨在解决当前解决方案的痛点，同时保留其产品API和优势。</em>
