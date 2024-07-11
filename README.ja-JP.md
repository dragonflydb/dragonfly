<p align="center">
  <a href="https://dragonflydb.io">
    <img  src="/.github/images/logo-full.svg"
      width="284" border="0" alt="Dragonfly">
  </a>
</p>

[![ci-tests](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml/badge.svg)](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml) [![Twitter URL](https://img.shields.io/twitter/follow/dragonflydbio?style=social)](https://twitter.com/dragonflydbio)

その他の言語:  [English](README.md) [简体中文](README.zh-CN.md) [한국어](README.ko-KR.md)

[Web サイト](https://www.dragonflydb.io/) • [ドキュメント](https://dragonflydb.io/docs) • [クイックスタート](https://www.dragonflydb.io/docs/getting-started) • [コミュニティ Discord](https://discord.gg/HsPjXGVH85) • [Dragonfly Forum](https://dragonfly.discourse.group/) • [Join the Dragonfly Community](https://www.dragonflydb.io/community)

[GitHub Discussions](https://github.com/dragonflydb/dragonfly/discussions) • [GitHub Issues](https://github.com/dragonflydb/dragonfly/issues) • [コントリビュート](https://github.com/dragonflydb/dragonfly/blob/main/CONTRIBUTING.md)

## 世界最速のインメモリデータストア

Dragonfly は最新のアプリケーションワークロードのために構築されたインメモリデータストアです。

Redis や Memcached の API と完全に互換性があるため、Dragonfly を採用するためにコードを変更する必要はありません。従来のインメモリデータストアと比較して、Dragonfly は 25 倍のスループット、より低いテールレイテンシでより高いキャッシュヒット率、そして容易な垂直スケーラビリティを提供します。

## コンテンツ

- [ベンチマーク](#ベンチマーク)
- [クイックスタート](https://github.com/dragonflydb/dragonfly/tree/main/docs/quick-start)
- [コンフィグ](#コンフィグ)
- [ロードマップとステータス](#ロードマップとステータス)
- [デザイン決定](#デザイン決定)
- [バックグラウンド](#バックグラウンド)

## <a name="ベンチマーク"><a/>ベンチマーク

<img src="http://static.dragonflydb.io/repo-assets/aws-throughput.svg" width="80%" border="0"/>

ベンチマークでは、Dragonfly は Redis と比較して 25 倍のスループットを示し、c6gn.16xlarge で 3.8M QPS を超えました。

Dragonfly のピークスループットにおける 99 パーセンタイルのレイテンシ指標:

| op    | r6g   | c6gn  | c7g   |
|-------|-------|-------|-------|
| set   | 0.8ms | 1ms   | 1ms   |
| get   | 0.9ms | 0.9ms | 0.8ms |
| setex | 0.9ms | 1.1ms | 1.3ms |

*すべてのベンチマークは `memtier_benchmark` (下記参照) を使い、スレッド数はサーバーとインスタンスタイプごとに調整しました。`memtier` は別の c6gn.16xlarge マシンで実行した。SETEX ベンチマークの有効期限は 500 に設定し、テストが終了しても有効であることを確認しました。*

```bash
  memtier_benchmark --ratio ... -t <threads> -c 30 -n 200000 --distinct-client-seed -d 256 \
     --expiry-range=...
```

パイプラインモード `--pipeline=30` では、Dragonfly は SET 操作で **10M QPS**、GET 操作で **15M QPS** に達する。

### Dragonfly vs. Memcached

AWS 上の c6gn.16xlarge インスタンスで Dragonfly と Memcached を比較した。

同程度のレイテンシで、Dragonfly のスループットは Memcached のスループットを書き込みと読み込みの両方のワークロードで上回った。Dragonfly は、[Memcached の書き込みパス](docs/memcached_benchmark.md)での競合により、書き込みワークロードでより優れたレイテンシを示しました。

#### SET ベンチマーク

| Server    | QPS(thousands qps) | latency 99% | 99.9%   |
|:---------:|:------------------:|:-----------:|:-------:|
| Dragonfly |  🟩 3844           |🟩 0.9ms     | 🟩 2.4ms |
| Memcached |   806              |   1.6ms     | 3.2ms    |

#### GET ベンチマーク

| Server    | QPS(thousands qps) | latency 99% | 99.9%   |
|-----------|:------------------:|:-----------:|:-------:|
| Dragonfly | 🟩 3717            |   1ms       | 2.4ms   |
| Memcached |   2100             |  🟩 0.34ms  | 🟩 0.6ms |


Memcached は読み取りベンチマークでより低いレイテンシを示したが、スループットも低かった。

### メモリ効率

メモリ効率をテストするために、`debug populate 5000000 key 1024` コマンドを使用して Dragonfly と Redis に ~5GB のデータを入れ、`memtier` コマンドで更新トラフィックを送信し、`bgsave` コマンドでスナップショットを開始しました。

この図は、各サーバがメモリ効率の面でどのような挙動を示したかを示している。

<img src="http://static.dragonflydb.io/repo-assets/bgsave-memusage.svg" width="70%" border="0"/>

Dragonfly はアイドル状態では Redis よりも 30% メモリ効率が高く、スナップショットフェーズではメモリ使用量の目に見える増加は見られなかった。ピーク時には Redis のメモリ使用量は Dragonfly の 3 倍近くまで増加しました。

Dragonfly はスナップショットをより早く、数秒以内に終了させました。

Dragonfly のメモリ効率の詳細については、[Dashtable ドキュメント](/docs/dashtable.md)を参照してください。



## <a name="コンフィグ"><a/>コンフィグ

Dragonfly は一般的な Redis の引数をサポートしています。例えば `dragonfly --requirepass=foo --bind localhost`。

Dragonfly は現在、以下の Redis 固有の引数をサポートしています:
 * `port`： Redis 接続ポート (`default: 6379`).
 * `bind`： ローカルホストからの接続のみを許可する場合は `localhost` を、**その IP** アドレスへの接続 (つまり外部からの接続) を許可する場合はパブリック IP アドレスを指定する。
 * `requirepass`： AUTH 認証用のパスワード (`default: ""`)。
 * `maxmemory`： データベースが使用するメモリの上限 (人間が読めるバイト数) (`default: 0`)。 `maxmemory` に `0` を指定すると、プログラムが自動的に最大メモリ使用量を決定する。
 * `dir`： Dragonfly Docker はデフォルトで `/data` フォルダをスナップショットに使用し、CLI は `""` を使用する。`v` の Docker オプションでホストフォルダにマッピングできる。
 * `dbfilename`： データベースを保存・ロードするファイル名 (`default: dump`).

Dragonfly 特有の議論もある:
 * `memcached_port`: Memcached 互換 API を有効にするポート (`default: disabled`)。
 * `keys_output_limit`: `keys` コマンドで返されるキーの最大数（`default: 8192`）。`keys` は危険なコマンドであることに注意してください。あまりに多くのキーを取得するとメモリ使用量が増大するため、結果を切り捨てています。
 * `dbnum`: `select` でサポートされるデータベースの最大数。
 * `cache_mode`: 以下の[斬新なキャッシュデザイン](#斬新なキャッシュデザイン)のセクションを参照してください。
 * `hz`: キーの有効期限評価頻度 (`default: 100`)。この頻度が低いと、アイドル時の CPU 使用量が少なくなるが、その分古くなったキーをクリアする速度が遅くなる。
 * `primary_port_http_enabled`: もし `true` (`default: true`) なら、メイン TCP ポートで HTTP コンソールにアクセスできるようにする。
 * `admin_port`: 割り当てられたポートのコンソールへの管理者アクセスを有効にする(`default: disabled`)。HTTP と RESP プロトコルの両方をサポートする。
 * `admin_bind`: 管理コンソールの TCP 接続を指定されたアドレスにバインドする(`default: any`)。HTTP と RESP の両方のプロトコルをサポートする。
 * `admin_nopass`: 割り当てられたポートで、認証トークンなしでコンソールへのオープン管理アクセスを有効にする (`default: false`)。HTTP と RESP の両方のプロトコルをサポートする。
 * `cluster_mode`: サポートするクラスターモード (`default: ""`)。現在は `emulated` のみをサポートしている。
 * `cluster_announce_ip`: クラスタコマンドがクライアントにアナウンスする IP。

### 一般的なオプションを使用した開始スクリプトの例:

```bash
./dragonfly-x86_64 --logtostderr --requirepass=youshallnotpass --cache_mode=true -dbnum 1 --bind localhost --port 6379  --maxmemory=12gb --keys_output_limit=12288 --dbfilename dump.rdb
```

また、`dragonfly --flagfile <filename>` を実行することで、設定ファイルから引数を指定することもできる。ファイルには 1 行に 1 つのフラグを記述し、キーと値のフラグには空白の代わりに等号を記述します。

ログの管理や TLS のサポートなど、その他のオプションについては `dragonfly --help` を実行してください。

## <a name="ロードマップとステータス"><a/>ロードマップとステータス

Dragonfly は現在、~185 個の Redis コマンドと、`cas` 以外のすべての Memcached コマンドをサポートしている。ほぼ Redis 5 API と同等ですが、Dragonfly の次のマイルストーンは基本的な機能を安定させ、レプリケーション API を実装することです。まだ実装されていないコマンドで必要なものがあれば、issue を開いてください。

Dragonfly ネイティブのレプリケーションについては、桁違いに高速な分散ログフォーマットを設計中です。

レプリケーション機能に続いて、Redis バージョン 3-6 の API に不足しているコマンドを追加していく予定です。

現在 Dragonfly がサポートしているコマンドについては、[コマンドリファレンス](https://dragonflydb.io/docs/category/command-reference)をご覧ください。

## <a name="デザイン決定"><a/> デザイン決定

### 斬新なキャッシュデザイン

Dragonfly には、シンプルでメモリ効率の良い、単一の統一された適応型キャッシュアルゴリズムがあります。

`cache_mode=true` フラグを渡すことでキャッシュモードを有効にすることができます。このモードをオンにすると、Dragonfly は将来つまずく可能性が最も低いアイテムを退避させますが、`maxmemory` の限界に近づいたときのみ退避させます。

### 比較的正確な有効期限

有効期限は 8 年以内。

ミリ秒精度の有効期限（PEXPIRE、PSETEX など）は、**2^28ms** を超える期限については、最も近い秒に丸められます。この誤差は 0.001% 以下であり、大きな範囲であれば許容範囲となります。

Dragonfly の期限と Redis の実装の詳細な違いについては、[こちら](docs/differences.md)を参照してください。

### ネイティブ HTTP コンソールと Prometheus 互換メトリクス

デフォルトでは、Dragonfly はメイン TCP ポート(6379)経由での HTTP アクセスを許可しています。その通り、Redis プロトコル経由でも HTTP プロトコル経由でも Dragonfly に接続することができます。ブラウザで試してみてください。HTTP アクセスには現在あまり情報がありませんが、将来的にはデバッグや管理に役立つ情報が含まれるようになる予定です。

Prometheus 互換のメトリクスを見るには、URL `:6379/metrics` にアクセスしてください。

Prometheus からエクスポートされたメトリクスは Grafana ダッシュボードと互換性があります[こちらを参照](tools/local/monitoring/grafana/provisioning/dashboards/dashboard.json)。


重要です！HTTP コンソールは安全なネットワーク内でアクセスすることを想定しています。Dragonfly の TCP ポートを外部に公開する場合は、`--http_admin_console=false` または `--nohttp_admin_console` でコンソールを無効にすることをお勧めします。


## <a name="バックグラウンド"><a/>バックグラウンド

Dragonfly は、インメモリデータストアを 2022 年に設計したらどのようになるかという実験から始まりました。メモリストアのユーザーとして、またクラウド企業で働いたエンジニアとしての経験から学んだ教訓をもとに、Dragonfly では 2 つの重要な特性を維持する必要があると考えました: それは、すべてのオペレーションにおける原子性の保証と、非常に高いスループットにおけるミリ秒以下の低レイテンシーです。

私たちの最初の課題は、パブリッククラウドで現在利用可能なサーバーを使用して、CPU、メモリー、I/O リソースをフルに活用する方法でした。これを解決するために、私たちは[シェアードナッシングアーキテクチャ](https://en.wikipedia.org/wiki/Shared-nothing_architecture)を使用しています。このアーキテクチャでは、各スレッドが辞書データのスライスを独自に管理できるように、スレッド間でメモリストアの鍵空間を分割することができます。これらのスライスを "shards" と呼ぶ。シェアードナッシングアーキテクチャのスレッドと I/O 管理のためのライブラリは、[こちら](https://github.com/romange/helio)でオープンソースで提供されています。

複数キー操作に対する原子性保証を提供するために、我々は最近の学術研究の進歩を利用している。Dragonfly のトランザクションフレームワークの開発には、論文 ["VLL: a lock manager redesign for main memory database systems"](https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf) を選びました。シェアードナッシングアーキテクチャと VLL の選択により、ミューテックスやスピンロックを使用せずにアトミックなマルチキー操作を構成することができました。これは我々の PoC にとって大きなマイルストーンであり、その性能は他の商用やオープンソースのソリューションよりも際立っていました。

私たちの第二の課題は、新しいストアのために、より効率的なデータ構造を設計することだった。この目標を達成するために、我々は論文 ["Dash: Scalable Hashing on Persistent Memory"](https://arxiv.org/pdf/2003.07302.pdf) に基づいたハッシュテーブル構造を核とした。この論文自体は、永続メモリ領域を中心にしており、メインメモリストアとは直接関係ありませんが、それでも私たちの問題に最も当てはまります。この論文で提案されているハッシュテーブル設計により、Redis の辞書に存在する 2 つの特別な特性を維持することができました: それは、データストアの成長中にハッシュをインクリメンタルする機能と、ステートレススキャン操作を使って変更中の辞書をトラバースする機能です。これら2つの特性に加え、Dash は CPU とメモリの使用効率が高い。Dash の設計を活用することで、私たちは以下のような機能をさらに革新することができました:
 * TTL レコードの効率的なレコード期限切れ。
 * LRU や LFU のような他のキャッシュ戦略よりも高いヒット率を、**ゼロメモリオーバーヘッド** で達成する新しいキャッシュエビクションアルゴリズム。
 * 新しい **フォークレス** スナップショットアルゴリズム。

Dragonfly の基盤を構築し、[そのパフォーマンスに満足したら](#ベンチマーク)、Redis と Memcached の機能を実装していきました。現在までに 185 個の Redis コマンド（Redis 5.0 API とほぼ同等）と 13 個の Memcached コマンドを実装しました。

そして最後に、<br>
<em>私たちの使命は、最新のハードウェアの進歩を活用した、クラウドワークロード向けの、優れた設計、超高速、コスト効率の良いインメモリデータストアを構築することです。現在のソリューションの API と提案を維持しながら、その問題点を解決するつもりです。
</em>
