<p align="center">
  <a href="https://dragonflydb.io">
    <img  src="/.github/images/logo-full.svg"
      width="284" border="0" alt="Dragonfly">
  </a>
</p>

[![ci-tests](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml/badge.svg)](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml) [![Twitter URL](https://img.shields.io/twitter/follow/dragonflydbio?style=social)](https://twitter.com/dragonflydbio)

> Antes de continuar, considere deixar uma estrela no nosso repositório ⭐️. Obrigado!

Outros idiomas: [简体中文](README.zh-CN.md) [日本語](README.ja-JP.md) [한국어](README.ko-KR.md)

[Site oficial](https://www.dragonflydb.io/) • [Documentação](https://dragonflydb.io/docs) • [Guia Rápido](https://www.dragonflydb.io/docs/getting-started) • [Discord da Comunidade](https://discord.gg/HsPjXGVH85) • [Fórum Dragonfly](https://dragonfly.discourse.group/) • [Participe da Comunidade](https://www.dragonflydb.io/community)

[Discussões no GitHub](https://github.com/dragonflydb/dragonfly/discussions) • [Issues no GitHub](https://github.com/dragonflydb/dragonfly/issues) • [Contribuindo](https://github.com/dragonflydb/dragonfly/blob/main/CONTRIBUTING.md) • [Dragonfly Cloud](https://www.dragonflydb.io/cloud)

## O armazenamento de dados em memória mais eficiente do mundo

Dragonfly é um armazenamento de dados em memória projetado para cargas de trabalho modernas.

Totalmente compatível com as APIs do Redis e Memcached, o Dragonfly não requer alterações de código para adoção. Em comparação com armazenamentos legados, o Dragonfly oferece 25x mais throughput, maiores taxas de acerto em cache com menor latência de cauda e pode operar com até 80% menos recursos para a mesma carga.

## Conteúdo

- [Benchmarks](#benchmarks)
- [Guia rápido](https://github.com/dragonflydb/dragonfly/tree/main/docs/quick-start)
- [Configuração](#configuration)
- [Roteiro e status](#roadmap-status)
- [Decisões de design](#design-decisions)
- [Contexto](#background)
- [Compilação a partir do código-fonte](./docs/build-from-source.md)

## <a name="benchmarks"><a/>Benchmarks

Primeiro comparamos o Dragonfly com o Redis em uma instância `m5.large`, frequentemente usada para rodar Redis devido à sua arquitetura single-threaded. O benchmark roda de outra instância de carga (c5n) na mesma AZ usando `memtier_benchmark  -c 20 --test-time 100 -t 4 -d 256 --distinct-client-seed`.

O Dragonfly mostra desempenho comparável:

1. SETs (`--ratio 1:0`):

| Redis                                 | DF                                   |
| ------------------------------------- | ------------------------------------ |
| QPS: 159K, P99.9: 1.16ms, P99: 0.82ms | QPS: 173K, P99.9: 1.26ms, P99: 0.9ms |

2. GETs (`--ratio 0:1`):

| Redis                                | DF                                   |
| ------------------------------------ | ------------------------------------ |
| QPS: 194K, P99.9: 0.8ms, P99: 0.65ms | QPS: 191K, P99.9: 0.95ms, P99: 0.8ms |

O benchmark mostra que a camada algorítmica do DF, que permite escalabilidade vertical, não gera sobrecarga significativa em execução single-thread.

Com uma instância mais forte (m5.xlarge), a diferença entre DF e Redis cresce.  
(`memtier_benchmark  -c 20 --test-time 100 -t 6 -d 256 --distinct-client-seed`):

1. SETs (`--ratio 1:0`):

| Redis                                 | DF                                    |
| ------------------------------------- | ------------------------------------- |
| QPS: 190K, P99.9: 2.45ms, P99: 0.97ms | QPS: 279K, P99.9: 1.95ms, P99: 1.48ms |

2. GETs (`--ratio 0:1`):

| Redis                                | DF                                    |
| ------------------------------------ | ------------------------------------- |
| QPS: 220K, P99.9: 0.98ms, P99: 0.8ms | QPS: 305K, P99.9: 1.03ms, P99: 0.87ms |

A capacidade de throughput do Dragonfly cresce com o tamanho da instância, enquanto o Redis single-thread atinge o limite de CPU.

<img src="http://static.dragonflydb.io/repo-assets/aws-throughput.svg" width="80%" border="0"/>

Na instância c6gn.16xlarge (maior capacidade de rede), o Dragonfly atinge 25x mais throughput que o Redis, superando 3.8M QPS.

Latência de 99% no pico de throughput do Dragonfly:

| op    | r6g   | c6gn  | c7g   |
| ----- | ----- | ----- | ----- |
| set   | 0.8ms | 1ms   | 1ms   |
| get   | 0.9ms | 0.9ms | 0.8ms |
| setex | 0.9ms | 1.1ms | 1.3ms |

_Todos os benchmarks foram realizados com `memtier_benchmark`, ajustando o número de threads conforme a instância. O `memtier` rodava em uma c6gn.16xlarge separada. No benchmark SETEX, foi definido tempo de expiração de 500 para garantir sobrevivência até o final do teste._

```bash
memtier_benchmark --ratio ... -t <threads> -c 30 -n 200000 --distinct-client-seed -d 256 \
   --expiry-range=...
```

Em modo pipeline `--pipeline=30`, o Dragonfly alcança **10M QPS** em SET e **15M QPS** em GET.

### Dragonfly vs. Memcached

Comparamos Dragonfly e Memcached em uma c6gn.16xlarge na AWS.

Com latência comparável, o throughput do Dragonfly superou o do Memcached tanto em leitura quanto escrita. Em escrita, a latência do Dragonfly foi melhor devido à contenção no [caminho de escrita do Memcached](docs/memcached_benchmark.md).

#### Benchmark de SET

| Servidor  | QPS (milhares) | latência 99% |  99.9%   |
| :-------: | :------------: | :----------: | :------: |
| Dragonfly |    🟩 3844     |   🟩 0.9ms   | 🟩 2.4ms |
| Memcached |      806       |    1.6ms     |  3.2ms   |

#### Benchmark de GET

| Servidor  | QPS (milhares) | latência 99% |  99.9%   |
| --------- | :------------: | :----------: | :------: |
| Dragonfly |    🟩 3717     |     1ms      |  2.4ms   |
| Memcached |      2100      |  🟩 0.34ms   | 🟩 0.6ms |

Memcached teve menor latência em leitura, mas também menor throughput.

### Eficiência de memória

Para testar a eficiência de memória, preenchemos o Dragonfly e o Redis com \~5GB de dados usando o comando `debug populate 5000000 key 1024`, enviamos tráfego de atualização com `memtier` e iniciamos o snapshot com o comando `bgsave`.

A figura abaixo demonstra como cada servidor se comportou em termos de eficiência de memória.

<img src="http://static.dragonflydb.io/repo-assets/bgsave-memusage.svg" width="70%" border="0"/>

O Dragonfly foi 30% mais eficiente em memória que o Redis em estado ocioso e não apresentou aumento visível no uso de memória durante a fase de snapshot. No pico, o uso de memória do Redis aumentou para quase 3 vezes o do Dragonfly.

O Dragonfly concluiu o snapshot mais rápido, em poucos segundos.

Para mais informações sobre eficiência de memória no Dragonfly, veja nosso [documento sobre Dashtable](/docs/dashtable.md).

## <a name="configuration"><a/>Configuração

O Dragonfly suporta argumentos comuns do Redis quando aplicável. Por exemplo, você pode executar: `dragonfly --requirepass=foo --bind localhost`.

Atualmente, o Dragonfly suporta os seguintes argumentos específicos do Redis:

- `port`: Porta de conexão Redis (`padrão: 6379`).
- `bind`: Use `localhost` para permitir conexões apenas locais ou um IP público para permitir conexões **para esse IP** (ou seja, externas também). Use `0.0.0.0` para permitir todas as conexões IPv4.
- `requirepass`: Senha para autenticação AUTH (`padrão: ""`).
- `maxmemory`: Limite de memória máxima (em bytes legíveis) usada pelo banco (`padrão: 0`). Um valor `0` significa que o programa determinará automaticamente o uso máximo de memória.
- `dir`: O Docker do Dragonfly usa a pasta `/data` para snapshots por padrão, o CLI usa `""`. Você pode usar a opção `-v` do Docker para mapear para uma pasta do host.
- `dbfilename`: Nome do arquivo para salvar/carregar o banco de dados (`padrão: dump`).

Também há argumentos específicos do Dragonfly:

- `memcached_port`: Porta para habilitar API compatível com Memcached (`padrão: desabilitado`).

- `keys_output_limit`: Número máximo de chaves retornadas no comando `keys` (`padrão: 8192`). Note que `keys` é um comando perigoso. Limitamos o resultado para evitar explosão de uso de memória ao buscar muitas chaves.

- `dbnum`: Número máximo de bancos de dados suportados para `select`.

- `cache_mode`: Veja a seção sobre [design de cache inovador](#novel-cache-design).

- `hz`: Frequência de avaliação de expiração de chave (`padrão: 100`). Frequências menores usam menos CPU em idle, mas têm menor taxa de remoção.

- `snapshot_cron`: Expressão cron para snapshots automáticos usando sintaxe cron padrão, com granularidade de minutos (`padrão: ""`).

  Exemplos:

  | Expressão Cron | Descrição                           |
  | -------------- | ----------------------------------- |
  | `* * * * *`    | A cada minuto                       |
  | `*/5 * * * *`  | A cada 5 minutos                    |
  | `5 */2 * * *`  | No minuto 5 de cada 2 horas         |
  | `0 0 * * *`    | Às 00:00 (meia-noite) todos os dias |
  | `0 6 * * 1-5`  | Às 06:00 (manhã) de segunda a sexta |

- `primary_port_http_enabled`: Permite acesso ao console HTTP na porta TCP principal se `true` (`padrão: true`).

- `admin_port`: Habilita acesso admin ao console na porta atribuída (`padrão: desabilitado`). Suporta protocolos HTTP e RESP.

- `admin_bind`: Define o IP de binding do console admin (`padrão: qualquer`). Suporta HTTP e RESP.

- `admin_nopass`: Habilita acesso admin sem autenticação (`padrão: false`). Suporta HTTP e RESP.

- `cluster_mode`: Modo cluster suportado (`padrão: ""`). Atualmente só `emulated`.

- `cluster_announce_ip`: IP que os comandos de cluster anunciam ao cliente.

- `announce_port`: Porta que os comandos de cluster anunciam ao cliente e ao master de replicação.

### Exemplo de script de inicialização com opções populares:

```bash
./dragonfly-x86_64 --logtostderr --requirepass=youshallnotpass --cache_mode=true -dbnum 1 --bind localhost --port 6379 --maxmemory=12gb --keys_output_limit=12288 --dbfilename dump.rdb
```

Argumentos também podem ser passados via:

- `--flagfile <arquivo>`: O arquivo deve conter um flag por linha, com `=` em vez de espaços para flags com valor. Não usar aspas.
- Variáveis de ambiente. Use `DFLY_x`, onde `x` é o nome exato do flag (case sensitive).

Para mais opções como logs ou suporte a TLS, execute `dragonfly --help`.

## <a name="roadmap-status"><a/>Roadmap e status

Atualmente o Dragonfly suporta \~185 comandos Redis e todos os comandos Memcached exceto `cas`. Já quase no nível da API do Redis 5, o próximo marco é estabilizar as funcionalidades básicas e implementar a API de replicação. Caso precise de um comando ainda não implementado, abra uma issue.

Para replicação nativa do Dragonfly, estamos projetando um formato de log distribuído que suportará velocidades ordens de magnitude maiores.

Após a replicação, continuaremos adicionando comandos faltantes das versões 3 a 6 do Redis.

Consulte nossa [Referência de Comandos](https://dragonflydb.io/docs/category/command-reference) para a lista atual.

## <a name="design-decisions"><a/>Decisões de design

### Design de cache inovador

O Dragonfly tem um algoritmo de cache adaptativo, unificado e simples, eficiente em memória.

Você pode habilitar o modo cache com o flag `--cache_mode=true`. Esse modo remove itens menos prováveis de serem acessados no futuro, mas **somente** próximo ao limite de `maxmemory`.

### Expiração com precisão relativa

Intervalos de expiração são limitados a \~8 anos.

Deadlines com precisão de milissegundos (PEXPIRE, PSETEX etc.) são arredondadas para o segundo mais próximo **quando superiores a 2^28ms**, com erro menor que 0.001%. Se isso for inadequado, entre em contato ou abra uma issue explicando o caso.

Para mais diferenças entre os deadlines do Dragonfly e do Redis, [clique aqui](docs/differences.md).

### Console HTTP nativo e métricas compatíveis com Prometheus

Por padrão, o Dragonfly permite acesso HTTP via porta TCP principal (6379). Ou seja, você pode conectar via protocolo Redis ou HTTP — o servidor reconhece automaticamente o protocolo ao conectar. Acesse com o navegador. Hoje o console HTTP tem pouca informação, mas no futuro incluirá debug e info de gerenciamento.

Acesse `:6379/metrics` para ver métricas Prometheus-compatíveis.

As métricas são compatíveis com o dashboard do Grafana, [veja aqui](tools/local/monitoring/grafana/provisioning/dashboards/dashboard.json).

Importante: o console HTTP deve ser acessado em rede segura. Se expor a porta TCP do Dragonfly externamente, desabilite o console com `--http_admin_console=false` ou `--nohttp_admin_console`.

## <a name="background"><a/>Contexto

O Dragonfly começou como um experimento para repensar um datastore in-memory em 2022. Baseado em lições como usuários e engenheiros de cloud, sabíamos que dois princípios deveriam ser preservados: garantias de atomicidade e latência sub-millisecond sob alto throughput.

Desafio 1: Utilizar ao máximo CPU, memória e I/O em servidores modernos. A solução foi adotar [arquitetura shared-nothing](https://en.wikipedia.org/wiki/Shared-nothing_architecture), particionando o keyspace entre threads. Chamamos os slices de “shards”. A biblioteca que gerencia threads e I/O foi open-sourceada [aqui](https://github.com/romange/helio).

Para garantir atomicidade em operações multi-key, usamos avanços recentes da pesquisa acadêmica. Escolhemos o paper ["VLL: a lock manager redesign for main memory database systems"](https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf) como base para o framework transacional. A combinação VLL + shared-nothing permitiu compor operações atômicas multi-key **sem mutex ou spinlock**. O resultado foi um PoC com performance superior a outras soluções.

Desafio 2: Estruturas de dados mais eficientes. Baseamos o hashtable no paper ["Dash: Scalable Hashing on Persistent Memory"](https://arxiv.org/pdf/2003.07302.pdf). Mesmo voltado à memória persistente, foi aplicável. O design permitiu manter:

- Hash incremental durante crescimento.
- Scan stateless mesmo com mudanças.

Além disso, o Dash é mais eficiente em uso de CPU/memória. Com esse design, inovamos ainda com:

- Expiração eficiente para registros TTL.
- Algoritmo de cache com mais hits que LRU/LFU com **zero overhead**.
- Algoritmo de snapshot **sem fork**.

Com essa base pronta e [performance satisfatória](#benchmarks), implementamos as APIs Redis e Memcached (\~185 comandos Redis, equivalente ao Redis 5.0, e 13 do Memcached).

Por fim, <br> <em>Nossa missão é construir um datastore in-memory rápido, eficiente e bem projetado para cargas em nuvem, aproveitando o hardware moderno. Queremos resolver as dores das soluções atuais mantendo APIs e propostas de valor.</em>
