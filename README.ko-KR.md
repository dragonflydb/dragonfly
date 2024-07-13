<p align="center">
  <a href="https://dragonflydb.io">
    <img  src="/.github/images/logo-full.svg"
      width="284" border="0" alt="Dragonfly">
  </a>
</p>

[![ci-tests](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml/badge.svg)](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml) [![Twitter URL](https://img.shields.io/twitter/follow/dragonflydbio?style=social)](https://twitter.com/dragonflydbio)

다른 언어 번역본:  [English](README.zh-CN.md) [简体中文](README.zh-CN.md) [日本語](README.ja-JP.md)

[Website](https://www.dragonflydb.io/) • [Docs](https://dragonflydb.io/docs) • [Quick Start](https://www.dragonflydb.io/docs/getting-started) • [Community Discord](https://discord.gg/HsPjXGVH85) • [Dragonfly Forum](https://dragonfly.discourse.group/) • [Join the Dragonfly Community](https://www.dragonflydb.io/community)

[GitHub Discussions](https://github.com/dragonflydb/dragonfly/discussions) • [GitHub Issues](https://github.com/dragonflydb/dragonfly/issues) • [Contributing](https://github.com/dragonflydb/dragonfly/blob/main/CONTRIBUTING.md) • [Dragonfly Cloud](https://www.dragonflydb.io/cloud)

## 세상에서 가장 빠른 인-메모리 스토어

Dragonfly는 현대 애플리케이션 작업을 위한 인-메모리 데이터스토어입니다.

Dragonfly는 Redis와 Memcached API와 완벽하게 호환되며, 이를 적용하기 위한 코드 변경을 필요로 하지 않습니다. Dragonfly는 기존 레거시 인-메모리 데이터스토어와 비교하여 25배 이상의 높은 처리량과 캐시 히트율, 낮은 꼬리 지연시간을 갖고있으며 간편한 수직 확장성을 지니고 있습니다.

## 콘텐츠

- [벤치마크](#benchmarks)
- [빠른 시작](https://github.com/dragonflydb/dragonfly/tree/main/docs/quick-start)
- [설정](#configuration)
- [로드맵과 상태](#roadmap-status)
- [설계 의사결정](#design-decisions)
- [개발 배경](#background)

## <a name="benchmarks"><a/>벤치마크

<img src="http://static.dragonflydb.io/repo-assets/aws-throughput.svg" width="80%" border="0"/>

벤치마크에 따르면, Dragonfly는 레디스와 비교하여 처리량이 25배이상 증가하였고, c6gn.16xlarge 인스턴스에서 3.8M QPS를 돌파하였음을 보여줍니다.

Dragonfly의 피크 처리량에서의 99퍼센트 지연 시간 지표:

| op    | r6g   | c6gn  | c7g   |
|-------|-------|-------|-------|
| set   | 0.8ms | 1ms   | 1ms   |
| get   | 0.9ms | 0.9ms | 0.8ms |
| setex | 0.9ms | 1.1ms | 1.3ms |

*모든 벤치마크는 서버 및 인스턴스 유형별로 조정된 스레드 수를 사용하여 `memtier_benchmark`(아래를 참고) 수행되었습니다. `memtier`는 별도의 c6gn.16xlarge 머신에서 실행되었습니다. 저희는 테스트 종료 이후에도 유효하게 유지되도록 보장하기 위해 SETEX 벤치마크의 만료 시간을 500으로 설정하였습니다.*

```bash
  memtier_benchmark --ratio ... -t <threads> -c 30 -n 200000 --distinct-client-seed -d 256 \
     --expiry-range=...
```

파이프라인 모드에서 `--pipeline=30`은 Dragonfly가 SET 연산으로 **10M QPS**, GET 연산으로 **15M QPS**에 도달할 수 있음을 나타냅니다.

### Dragonfly vs. Memcached

저희는 AWS의 c6gn.16xlarge 인스턴스에서 Dragonfly와 Memcached를 비교하는 작업을 수행했습니다.

비슷한 지연시간을 가진 상황에서, Dragonfly의 처리량은 쓰기 및 읽기 작업 모두에서 Memcached보다 성능이 뛰어났습니다. 쓰기 작업에서는 [Memcached의 쓰기 경로](docs/memcached_benchmark.md)에서의 경합으로 인하여 Dragonfly가 보다 적은 지연시간을 보였다는 점이 입증되었습니다.

#### SET 벤치마크

| Server    | QPS(thousands qps) | latency 99% | 99.9%   |
|:---------:|:------------------:|:-----------:|:-------:|
| Dragonfly |  🟩 3844           |🟩 0.9ms     | 🟩 2.4ms |
| Memcached |   806              |   1.6ms     | 3.2ms    |

#### GET 벤치마크

| Server    | QPS(thousands qps) | latency 99% | 99.9%   |
|-----------|:------------------:|:-----------:|:-------:|
| Dragonfly | 🟩 3717            |   1ms       | 2.4ms   |
| Memcached |   2100             |  🟩 0.34ms  | 🟩 0.6ms |

Memcached는 읽기 벤치마크의 지연 시간은 적었지만, 처리량도 낮았습니다.

### 메모리 효율

메모리 효율을 테스트하기 위해서, 저희는 `debug populate 5000000 key 1024` 명령어를 활용하여 Dragonfly와 Redis에 ~5GB 정도의 데이터를 채운 후, `memtier` 를 통하여 업데이트 트래픽을 전송한 후, `bgsave` 명령을 통하여 스냅샷을 시작했습니다.

이 그림은 메모리 효율 측면에서 각 서버가 어떻게 동작했는지 보여줍니다.

<img src="http://static.dragonflydb.io/repo-assets/bgsave-memusage.svg" width="70%" border="0"/>

Dragonfly는 유휴 상태에서 Redis보다 메모리 효율이 30% 더 좋았으며, 스냅샷 단계에서 메모리 사용량이 눈에 띄게 증가하지 않았습니다. Redis는 고점에서 Dragonfly에 비해 메모리 사용량이 약 3배 증가하였습니다.

Dragonfly는 스냅샷 단계를 몇 초안에 더 빨리 마쳤습니다.

Dragonfly의 메모리 효율에 대한 정보가 더 필요하시다면, 저희의 [Dastable 문서](/docs/dashtable.md)를 참고하시기 바랍니다.


## <a name="configuration"><a/>설정

Dragonfly는 적용 가능한 Redis 인수를 지원합니다. 예를 들면, `dragonfly --requirepass=foo --bind localhost`와 같은 명령어를 사용할 수 있습니다.

Dragonfly는 현재 아래와 같은 Redis 인수들을 지원합니다 :
  * `port`: Redis 연결 포트 (`기본값: 6379`).
  * `bind`: `localhost`를 사용하여 로컬호스트 연결만 허용하거나 공용 IP 주소를 사용하여 해당 IP 주소에 연결을 허용합니다.(즉, 외부에서도 가능)
  * `requirepass`: AUTH 인증을 위한 패스워드 (`기본값: ""`).
  * `maxmemory`: 데이터베이스에서 사용하는 최대 메모리 제한(사람이 읽을 수 있는 바이트 단위) (`기본값: 0`). `maxmemory` 의 값이 `0` 이면 프로그램이 최대 메모리 사용량을 자동으로 결정합니다.
  * `dir`: Dragonfly Docker는 스냅샷을 위해 기본적으로 `/data` 폴더를 사용하고, CLI은 `""`을 사용합니다. Docker 옵션인 `-v` 을 통해서 호스트 폴더에 매핑할 수 있습니다.
  * `dbfilename`: 저장하고 불러올 데이터베이스 파일 이름 (`기본값: dump`).

아래는 Dragonfly 전용 인수 입니다 :
  * `memcached_port`: Memcached 호환 API를 위한 포트 (`기본값: disabled`).
  * `keys_output_limit`: `keys` 명령을 통해 반환 되는 최대 키의 수 (`기본값: 8192`). `keys` 명령은 위험하기 때문에, 너무 많은 키를 가져올 때 메모리 사용량이 급증하지 않도록 결과를 해당 인수만큼 잘라냅니다.
  * `dbnum`: `select` 명령에 대해 지원되는 최대 데이터베이스 수.
  * `cache_mode`: 아래의 섹션 [새로운 캐시 설계](#novel-cache-design)을 참고해주시기 바랍니다.
  * `hz`: 키가 만료되었는지를 판단하는 빈도(`기본값: 100`). 낮은 빈도는 키 방출이 느려지는 대신, 유휴 상태일 때 CPU 사용량을 줄입니다.
  * `primary_port_http_enabled`: `true` 인 경우 HTTP 콘솔로 메인 TCP 포트 접근을 허용합니다. (`기본값: true`).
  * `admin_port`: 할당된 포트에서 관리자 콘솔 접근을 활성화합니다. (`기본값: disabled`). HTTP와 RESP 프로토콜 모두를 지원합니다.
  * `admin_bind`: 주어진 주소에 관리자 콘솔 TCP 연결을 바인딩합니다. (`기본값: any`). HTTP와 RESP 프로토콜 모두를 지원합니다.
  * `admin_nopass`: 할당된 포트에 대해서 인증 토큰 없이 관리자 콘솔 접근을 활성화합니다. (`default: false`). HTTP와 RESP 프로토콜 모두를 지원합니다.
  * `cluster_mode`: 클러스터 모드가 지원됩니다. (`기본값: ""`). 현재는`emulated` 만 지원합니다.
  * `cluster_announce_ip`: 클러스터 명령을 클라이언트에게 알리는 IP 주소.


### 주요 옵션을 활용한 실행 스크립트 예시:

```bash
./dragonfly-x86_64 --logtostderr --requirepass=youshallnotpass --cache_mode=true -dbnum 1 --bind localhost --port 6379  --maxmemory=12gb --keys_output_limit=12288 --dbfilename dump.rdb
```

인수들은 `dragonfly --flagfile <filename>`을 실행하여 설정 파일을 통해서도 전달할 수 있습니다. 전달될 파일은 각 줄에 키-값 형태의 플래그 나열 하기위해 등호를 사용합니다.

로그 관리나 TLS 지원과 같은 추가 옵션을 확인하고 싶다면, `dragonfly --help` 를 실행해보시길 바랍니다.

## <a name="roadmap-status"><a/>로드맵과 상태

Dragonfly는 현재 ~185개의 Redis 명령어들과 `cas` 뿐만 아니라 모든 Memcached 명령어를 지원합니다. 이는 거의 Redis 5 API와 동등하며, Dragonfly의 다음 마일스톤은 기본 기능 을 안정화하고 복제 API를 구현하는 것입니다. 아직 구현되지 않은 필요한 명령가 있다면, 이슈를 오픈해주세요.

Draginfly 고유 복제기능을 위해, 저희는 몇 배 높은 속도를 지원할 수 있는 분산 로그 형식을 설계하고 있습니다.

복제 기능을 추가한 뒤에 저희는 Redis 3-6 API에 해당되는 누락 명령어들을 계속 추가할 예정입니다.

Dragonfly에 의해 현재 지원되는 명령어를 확인하기 위해서 [명령어 레퍼런스](https://dragonflydb.io/docs/category/command-reference)를 참고해주시기 바랍니다.

## <a name="design-decisions"><a/>설계 의사결정

### 새로운 캐시 설계

Dragonfly는 단순하고 메모리 효율적인 단일, 통합, 적응형 캐싱 알고리즘을 제공합니다.

`--cache_mode=true` 플래그를 전달하여 캐싱 모드를 활성화할 수 있습니다. 이 모드가 활성화되면, Dragonfly는 `maxmemory` 한도에 가까워질 때만, 미래에 재사용 될 가능성이 가장 낮은 항목을 방출합니다.

### 상대적인 정확성을 가진 만료 기한

만료 범위는 약 ~8년으로 제한됩니다.

밀리초 단위의 정밀한 만료 기한(PEXPIRE, PSETEX, 등)은 **2^28ms보다 큰 기한에 대해** 가장 가까운 초로 반올림됩니다. 이는 0.001% 미만의 오차를 가지며, 큰 범위에 대해 적용될 때는 수용 가능한 수준입니다. 만약 이런 방식이 사용사례에 적합하지 않다면, 문의를 주시거나 해당 사용사례를 설명하는 이슈를 오픈해주세요.

Dragonfly와 Redis의 만료 기한에 대한 구현의 차이는 [여기서 확인하실 수 있습니다](docs/differences.md).

### 네이티브 HTTP 콘솔과 Prometheus 호환 매트릭

기본적으로, Dragonfly는 메인 TCP 포트(6379)에 HTTP 접근을 허용합니다. 즉, Redis 프로토콜과 HTTP 프로토콜 모두를 통해 Dragonfly에 연결할 수 있습니다. - 서버는 연결 초기화 과정에서 프로토콜을 자동으로 인식합니다. 웹 브라우저를 통하여 시도해보시기 바랍니다. 현재 HTTP 접근은 많은 정보를 제공하지 않지만, 유용한 디버깅 및 관리 정보를 향후 추가할 예정입니다.

`:6379/metrics` 에 접근하게 되면, Prometheus 호환 매트릭을 확인할 수 있습니다.

Prometheus에서 내보내는 매트릭들은 Grafana 대시보드와 호환됩니다. 자세한 내용은 [여기](tools/local/monitoring/grafana/provisioning/dashboards/dashboard.json)를 참조해주세요.

중요! HTTP 노솔은 안전한 네트워크 내에서 접근하도록 설계되었습니다. Dragonfly의 TCP 포트를 외부로 노출한다면, `--http_admin_console=false` 혹은 `--nohttp_admin_console`과 같은 인수를 활용하여 콘솔을 비활성화하는 것을 조언해드립니다.


## <a name="background"><a/>개발배경

Dragonfly는 2022년에 인-메모리 데이터스토어를 설계한다면 어땠을까에 대한 실험으로 시작되었습니다. 클라우드 회사에서 근무한 엔지니어 및 메모리 스토어 사용자의 경험을 바탕으로, 저희는 Dragonfly에 핵심적인 두 가지 핵심 특성을 보존해야함을 알았습니다: 모든 작업에 대한 원자성 보장과 매우 높은 처리량에 대한 밀리초 이하의 낮은 지연 시간을 보장하는 것이었습니다.

첫 번째 문제는 오늘날 퍼블릭 클라우드 환경에서 사용 가능한 서버를 사용하여 CPU, 메모리 및 I/O 자원을 어떻게 최대한 활용할 수 있을지였습니다. 이 문제를 해결하기 위해 저희는 [비공유 아키텍처(Shared Nothing Architecture)](https://en.wikipedia.org/wiki/Shared-nothing_architecture)를 사용했습니다. 이는 저희가 메모리 스토어의 각 스레드 사이의 키 공간을 분할할 수 있게하였습니다. 이를 통해 각 스레드들은 그들의 딕셔너리 데이터들의 조각을 관리할 수 있게 되었습니다. 저희는 이 조각들을 "샤드(shards)"라 불렀습니다. 비공유 아키텍처에 대한 스레드 및 I/O 관리를 위한 라이브러리는 [여기](https://github.com/romange/helio)에서 오픈소스로 제공됩니다.

멀티-키 작업에 대한 원자성 보장을 위해, Dragonfly의 트랜잭션 프레임워크를 개발하기 위해 저희는 최근 학계의 연구 발전을 활용했고 ["VLL: a lock manager redesign for main memory database systems”](https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf) 논문을 채택했습니다. 비공유 아키텍처와 VLL의 선택은 우리가 뮤텍스나 스핀락을 사용하지 않고도 원자적 멀티-키 작업을 구성할 수 있게 했습니다.
이것은 저희의 PoC에 있어서 주요한 마일스톤이었고, 그 성능은 다른 상용 및 오픈소스 솔루션보다 성능이 뛰어났습니다.

두 번째 문제는 새로운 저장소를 위하여 더 효율적인 데이터 구조를 설계하는 것이었습니다. 이 목표를 달성하기 위해서 저희는 핵심 해시테이블 구조를 ["Dash: Scalable Hashing on Persistent Memory"](https://arxiv.org/pdf/2003.07302.pdf) 논문을 기반으로 작업했습니다. 이 논문은 영속적인 메모리 도메인을 중심으로 다루며, 이는 메인-메모리 저장소와 직접적인 연관관계는 없었습니다. 하지만 여전히 저희 문제를 해결하기 위해서 가장 적합했습니다. 해당 논문의 제안된 해시테이블 설계는 저희가 레디스 딕셔너리에 표현된 두 가지 특별한 특성을 유지 가능하게 해줬습니다: 데이터스토어 확장 중 증분 해싱 기능과 상태 없는 스캔 작업을 사용하여 변화하는 딕셔너리를 순회하는 능력이었습니다. 이 두 가지 속성 외에도 Dash는 CPU와 메모리 사용에서 더 효율적입니다. 저희는 다음과 같은 기능들로 더욱 혁신할 수 있었습니다:
 * TTL 레코드에 대한 효율적인 만료 처리
 * LRU와 LFU 같은 다른 캐시 전략보다 더 높은 히트율을 달성하는 새로운 캐시 방출 알고리즘과 **제로 메모리 오버헤드**.
 * 새로운 **fork-less** 스냅샷 알고리즘.

저희는 Dragonfly의 기반을 구축하고 성능에 만족하게 되었을 때, Redis와 Memcached의 기능을 구현하기 시작했습니다. 저희는 약 185개의 Redis 명령(대략적으로 Redis 5.0 API와 동등)과 13개의 Memecached 명령을 구현했습니다.

마지막으로, <br>
<em>저희의 임무는 최신 하드웨어 발전을 활용하는 클라우드 작업을 위한 멋진 설계와 초고속 처리량 그리고 비용효율적인 인-메모리 데이터스토어를 만드는 것입니다. 저희는 현재 솔루션의 제품 API들이나 제안을 유지하면서 당면 과제를 해결하고자 합니다.
</em>
