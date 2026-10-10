<p align="center">
  <a href="https://dragonflydb.io">
    <img  src="/.github/images/logo-full.svg"
      width="284" border="0" alt="Dragonfly">
  </a>
</p>

[![ci-tests](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml/badge.svg)](https://github.com/dragonflydb/dragonfly/actions/workflows/ci.yml) [![Total pulls](https://img.shields.io/endpoint?url=https%3A%2F%2Fstorage.googleapis.com%2Fstatic.dragonflydb.io%2Frepo-assets%2Fghcr-downloads%2Ftotal.json)](https://github.com/dragonflydb/dragonfly/pkgs/container/dragonfly) [![Monthly pulls](https://img.shields.io/endpoint?url=https%3A%2F%2Fstorage.googleapis.com%2Fstatic.dragonflydb.io%2Frepo-assets%2Fghcr-downloads%2Fmonthly.json)](https://github.com/dragonflydb/dragonfly/pkgs/container/dragonfly) [![Twitter URL](https://img.shields.io/twitter/follow/dragonflydbio?style=social)](https://twitter.com/dragonflydbio)

> Sebelum lanjut membaca, dukung kami dengan memberikan bintang ⭐️ di GitHub. Terima kasih!

Bahasa lain: [English](README.md) [简体中文](README.zh-CN.md) [日本語](README.ja-JP.md) [한국어](README.ko-KR.md) [Português](README.pt-BR.md) [ภาษาไทย](README.th-TH.md)

[Situs Web](https://www.dragonflydb.io/) • [Dokumentasi](https://dragonflydb.io/docs) • [Panduan Cepat](https://www.dragonflydb.io/docs/getting-started) • [Komunitas Discord](https://discord.gg/HsPjXGVH85) • [Dragonfly User Conference](https://www.dragonflydb.io/events/dragonfly-ascent) • [Gabung Komunitas Dragonfly](https://www.dragonflydb.io/community)

[Diskusi GitHub](https://github.com/dragonflydb/dragonfly/discussions) • [Laporan Masalah (Issues)](https://github.com/dragonflydb/dragonfly/issues) • [Panduan Kontribusi](https://github.com/dragonflydb/dragonfly/blob/main/CONTRIBUTING.md) • [Panduan Agen AI](AGENTS.md) • [Dragonfly Cloud](https://www.dragonflydb.io/cloud)

## Datastore in-memory paling efisien di dunia

Dragonfly adalah *in-memory data store* yang dirancang khusus untuk beban kerja aplikasi modern.

Sepenuhnya kompatibel dengan API Redis dan Memcached, Dragonfly dapat langsung digunakan tanpa perlu mengubah kode aplikasi Anda. Dibandingkan dengan datastore in-memory konvensional, Dragonfly mampu menghasilkan *throughput* hingga 25 kali lipat lebih tinggi, rasio *cache hit* lebih optimal dengan *tail latency* yang sangat rendah, serta dapat menghemat penggunaan sumber daya hingga 80% untuk beban kerja berukuran sama.

## Daftar Isi

- [Tolok Ukur Kinerja (Benchmarks)](#tolok-ukur-kinerja-benchmarks)
- [Panduan Cepat (Quick Start)](https://github.com/dragonflydb/dragonfly/tree/main/docs/quick-start)
- [Konfigurasi](#konfigurasi)
- [Keputusan Desain](#keputusan-desain)
- [Latar Belakang](#latar-belakang)
- [Kompilasi dari Kode Sumber (Build from source)](./docs/build-from-source.md)
- [Kontributor](#kontributor)

## <a name="tolok-ukur-kinerja-benchmarks"></a>Tolok Ukur Kinerja (Benchmarks)

Pertama, kami membandingkan Dragonfly dengan Redis pada *instance* `m5.large`, tipe server yang umum digunakan untuk menjalankan Redis karena arsitekturnya yang *single-threaded*. Program *benchmark* dijalankan dari *instance load-test* terpisah (c5n) di Availability Zone (AZ) yang sama menggunakan perintah:
`memtier_benchmark -c 20 --test-time 100 -t 4 -d 256 --distinct-client-seed`

Dragonfly menunjukkan performa yang sebanding:

1. Operasi SET (`--ratio 1:0`):

| Redis                                    | DF                                     |
| ---------------------------------------- | -------------------------------------- |
| QPS: 159K, P99.9: 1.16ms, P99: 0.82ms    | QPS: 173K, P99.9: 1.26ms, P99: 0.9ms   |

2. Operasi GET (`--ratio 0:1`):

| Redis                                    | DF                                     |
| ---------------------------------------- | -------------------------------------- |
| QPS: 194K, P99.9: 0.8ms, P99: 0.65ms     | QPS: 191K, P99.9: 0.95ms, P99: 0.8ms   |

Hasil tolok ukur di atas membuktikan bahwa lapisan algoritmik internal Dragonfly—yang memungkinkannya melakukan *scale vertically* (skala vertikal)—tidak membebani performa saat berjalan dalam mode *single-threaded*.

Namun, ketika beralih ke *instance* yang lebih besar (`m5.xlarge`), selisih performa antara DF dan Redis mulai melebar secara signifikan:
(`memtier_benchmark -c 20 --test-time 100 -t 6 -d 256 --distinct-client-seed`):

1. Operasi SET (`--ratio 1:0`):

| Redis                                    | DF                                     |
| ---------------------------------------- | -------------------------------------- |
| QPS: 190K, P99.9: 2.45ms, P99: 0.97ms    | QPS: 279K, P99.9: 1.95ms, P99: 1.48ms  |

2. Operasi GET (`--ratio 0:1`):

| Redis                                    | DF                                     |
| ---------------------------------------- | -------------------------------------- |
| QPS: 220K, P99.9: 0.98ms, P99: 0.8ms     | QPS: 305K, P99.9: 1.03ms, P99: 0.87ms  |

Kapasitas *throughput* Dragonfly terus meningkat seiring bertambahnya ukuran *instance*, sedangkan Redis yang *single-threaded* terbentur batas kemampuan CPU (*CPU bottleneck*) dan mencapai titik maksimal lokal performanya.

<img src="http://static.dragonflydb.io/repo-assets/aws-throughput.svg" width="80%" border="0"/>

Saat dibandingkan pada *instance* berkemampuan jaringan tertinggi `c6gn.16xlarge`, Dragonfly mencatatkan lonjakan *throughput* hingga **25 kali lipat** dibanding satu proses Redis tunggal, menembus angka **3,8 juta QPS**.

Metrik latensi persentil ke-99 (P99) Dragonfly pada puncak *throughput*:

| Operasi | r6g   | c6gn  | c7g   |
| ------- | ----- | ----- | ----- |
| set     | 0.8ms | 1ms   | 1ms   |
| get     | 0.9ms | 0.9ms | 0.8ms |
| setex   | 0.9ms | 1.1ms | 1.3ms |

*Seluruh pengujian dilakukan menggunakan `memtier_benchmark` (lihat di bawah) dengan jumlah thread yang disesuaikan untuk setiap server dan tipe instance. `memtier` dijalankan pada mesin c6gn.16xlarge terpisah. Waktu kedaluwarsa diatur ke 500 pada pengujian SETEX untuk memastikan data tidak terhapus sebelum tes selesai.*

```bash
memtier_benchmark --ratio ... -t <threads> -c 30 -n 200000 --distinct-client-seed -d 256 \
   --expiry-range=...
```

Dalam mode *pipeline* (`--pipeline=30`), Dragonfly mampu mencapai **10 juta QPS** untuk operasi SET dan **15 juta QPS** untuk operasi GET.

### Dragonfly vs. Memcached

Kami juga membandingkan Dragonfly dengan Memcached pada *instance* `c6gn.16xlarge` di AWS.

Dengan latensi yang setara, *throughput* Dragonfly melampaui Memcached baik pada beban kerja tulis (*write*) maupun baca (*read*). Dragonfly juga mencatatkan latensi yang lebih baik pada operasi tulis karena tidak mengalami kendala kontensi pada [jalur penulisan Memcached](docs/memcached_benchmark.md).

#### Tolok Ukur Operasi SET

| Server    | QPS (ribuan QPS) | Latensi P99 | P99.9   |
| :-------: | :--------------: | :---------: | :-----: |
| Dragonfly |     🟩 3844      |  🟩 0.9ms   | 🟩 2.4ms |
| Memcached |       806        |    1.6ms    |  3.2ms  |

#### Tolok Ukur Operasi GET

| Server    | QPS (ribuan QPS) | Latensi P99 | P99.9   |
| :-------: | :--------------: | :---------: | :-----: |
| Dragonfly |     🟩 3717      |     1ms     |  2.4ms  |
| Memcached |       2100       |  🟩 0.34ms  | 🟩 0.6ms |

Memcached mencatat latensi yang sedikit lebih rendah pada pengujian baca, namun menghasilkan total *throughput* yang jauh lebih kecil.

### Efisiensi Memori

Untuk menguji efisiensi memori, kami mengisi Dragonfly dan Redis dengan data sebesar ~5GB menggunakan perintah `debug populate 5000000 key 1024`, mengirim beban pembaruan data secara berkala menggunakan `memtier`, lalu memicu proses *snapshotting* dengan perintah `bgsave`.

Grafik berikut menunjukkan perbandingan perilaku konsumsi memori dari masing-masing server:

<img src="http://static.dragonflydb.io/repo-assets/bgsave-memusage.svg" width="70%" border="0"/>

Dragonfly terbukti **30% lebih hemat memori** daripada Redis saat kondisi *idle* (diam), dan sama sekali tidak menunjukkan lonjakan memori yang signifikan selama fase pembuatan *snapshot*. Sebagai perbandingan, saat beban puncak pembuatan snapshot, konsumsi memori Redis melonjak hingga hampir 3 kali lipat dibanding Dragonfly.

Selain itu, Dragonfly menyelesaikan proses *snapshot* jauh lebih cepat, hanya dalam hitungan beberapa detik.

Penjelasan mendalam mengenai efisiensi memori Dragonfly dapat dibaca di dokumen [Dashtable](/docs/dashtable.md).

---

## <a name="konfigurasi"></a>Konfigurasi

Dragonfly mendukung sebagian besar argumen umum Redis. Contoh cara menjalankan:
```bash
dragonfly --requirepass=foo --bind localhost
```

Argumen bawaan Redis yang saat ini didukung oleh Dragonfly meliputi:
* `port`: Port koneksi Redis (`default: 6379`).
* `bind`: Gunakan `localhost` untuk hanya menerima koneksi lokal, atau tentukan alamat IP publik untuk membatasi koneksi **hanya ke IP tersebut**. Gunakan `0.0.0.0` untuk membuka akses ke semua antarmuka IPv4.
* `requirepass`: Kata sandi untuk autentikasi perintah `AUTH` (`default: ""`).
* `maxmemory`: Batas memori maksimum yang dialokasikan (dalam format byte yang mudah dibaca, contoh: `12gb`) (`default: 0`). Nilai `0` menandakan program akan menentukan batas alokasi memori secara otomatis.
* `dir`: Secara default, container Docker Dragonfly menggunakan folder `/data` untuk menyimpan berkas snapshot, sedangkan mode CLI menggunakan direktori aktif saat ini (`""`). Gunakan opsi Docker `-v` untuk menghubungkannya ke folder di komputer host Anda.
* `dbfilename`: Nama file untuk menyimpan dan memuat database snapshot (`default: dump`).

Argumen khusus Dragonfly tambahan:
* `memcached_port`: Port untuk mengaktifkan API yang kompatibel dengan Memcached (`default: dinonaktifkan`).
* `keys_output_limit`: Jumlah maksimum *key* yang dikembalikan oleh perintah `keys` (`default: 8192`). Perlu diingat bahwa perintah `keys` berisiko tinggi; Dragonfly membatasi hasilnya agar tidak terjadi ledakan konsumsi memori saat mengambil terlalu banyak data sekaligus.
* `dbnum`: Jumlah maksimum database logis yang didukung untuk perintah `select`.
* `cache_mode`: Lihat penjelasan pada bagian [desain cache inovatif](#desain-cache-inovatif) di bawah.
* `hz`: Frekuensi evaluasi masa kedaluwarsa kunci (*key expiry*) (`default: 100`). Frekuensi yang lebih rendah menghemat penggunaan CPU saat idle, dengan konsekuensi laju pembersihan memori (*eviction rate*) yang sedikit lebih lambat.
* `snapshot_cron`: Ekspresi penjadwalan cron untuk pencadangan (*snapshot*) otomatis berkala dengan presisi menit (`default: ""`).
  Penjelasan lebih rinci tersedia di [dokumentasi resmi kami](https://www.dragonflydb.io/docs/managing-dragonfly/backups#the-snapshot_cron-flag).

  | Ekspresi Jadwal Cron | Keterangan                                  |
  | -------------------- | ------------------------------------------- |
  | `* * * * *`          | Setiap menit                                |
  | `*/5 * * * *`        | Setiap kelipatan menit ke-5                 |
  | `5 */2 * * *`        | Menit ke-5 pada setiap kelipatan jam genap  |
  | `0 0 * * *`          | Tepat pukul 00:00 (tengah malam) setiap hari |
  | `0 6 * * 1-5`        | Pukul 06:00 pagi setiap hari Senin s.d. Jumat |

* `primary_port_http_enabled`: Mengizinkan akses konsol HTTP pada port TCP utama jika bernilai `true` (`default: true`).
* `admin_port`: Mengaktifkan akses konsol admin pada port khusus (`default: dinonaktifkan`). Mendukung protokol HTTP dan RESP.
* `admin_bind`: Menentukan alamat IP untuk koneksi TCP konsol admin (`default: any`).
* `admin_nopass`: Mengizinkan akses langsung ke konsol admin tanpa token autentikasi (`default: false`).
* `cluster_mode`: Mengaktifkan mode klaster (`default: ""`). Mendukung opsi `emulated` dan `yes`.
* `cluster_announce_ip`: Alamat IP yang diumumkan ke klien untuk perintah cluster.
* `announce_port`: Port yang diumumkan ke klien dan ke node master replikasi.

### Contoh Skrip Menjalankan dengan Opsi Populer:

```bash
./dragonfly-x86_64 --logtostderr --requirepass=youshallnotpass --cache_mode=true -dbnum 1 --bind localhost --port 6379 --maxmemory=12gb --keys_output_limit=12288 --dbfilename dump
```

Argumen konfigurasi juga dapat dimasukkan melalui:
* `--flagfile <nama_file>`: Berkas teks yang mencantumkan satu argumen per baris menggunakan tanda sama dengan (`=`) tanpa spasi (tidak perlu tanda petik).
* Variabel Lingkungan (*Environment Variables*): Gunakan format `DFLY_x`, di mana `x` adalah nama flag persis (bersifat *case-sensitive*).

Untuk melihat opsi lengkap seperti pengelolaan log atau konfigurasi TLS, jalankan:
```bash
dragonfly --help
```

---

## <a name="keputusan-desain"></a>Keputusan Desain

### Desain Cache Inovatif

Dragonfly menggunakan algoritma caching adaptif tunggal yang terpadu, sederhana, dan sangat hemat memori.

Mode caching dapat diaktifkan dengan menambahkan flag `--cache_mode=true`. Saat mode ini aktif, Dragonfly secara otomatis akan menghapus data yang memiliki probabilitas terendah untuk diakses kembali di masa mendatang, dan pembersihan ini hanya dilakukan saat penggunaan memori mendekati batas `maxmemory`.

### Batas Kedaluwarsa dengan Presisi Relatif

Rentang masa aktif kunci (*expiry range*) dibatasi hingga sekitar ~8 tahun.

Batas kedaluwarsa dengan presisi milidetik (seperti `PEXPIRE`, `PSETEX`, dsb.) akan dibulatkan ke detik terdekat **khusus untuk durasi yang lebih besar dari 2^28 milidetik** (sekitar 3 hari). Pendekatan ini menghasilkan tingkat kesalahan di bawah 0,001%, sangat aman dan dapat diterima untuk rentang waktu yang panjang. Jika kebutuhan aplikasi Anda membutuhkan presisi absolut, silakan hubungi tim kami atau buat issue baru.

Perbedaan komprehensif antara sistem kedaluwarsa Dragonfly dan implementasi Redis dapat dilihat di [dokumen perbedaan ini](docs/differences.md).

### Konsol HTTP Bawaan dan Metrik Kompatibel Prometheus

Secara default, Dragonfly membuka akses HTTP langsung melalui port TCP utamanya (6379). Artinya, Anda dapat terhubung ke Dragonfly menggunakan protokol Redis maupun melalui browser via HTTP biasa—server akan mendeteksi jenis protokol secara otomatis saat *handshake* awal koneksi.

Buka alamat `:6379/metrics` pada browser atau scraper Anda untuk melihat metrik pemantauan yang kompatibel dengan format Prometheus.

Metrik yang diekspor kompatibel penuh dengan dashboard visualisasi Grafana; contoh template dashboard dapat dilihat di [tautan ini](tools/local/monitoring/grafana/provisioning/dashboards/dragonfly.json).

> ⚠️ **Penting**: Konsol HTTP dirancang untuk diakses dalam jaringan internal yang aman. Jika port TCP Dragonfly terbuka langsung ke internet publik, sangat disarankan menonaktifkan konsol HTTP menggunakan flag `--primary_port_http_enabled=false` atau `--noprimary_port_http_enabled`.

---

## <a name="latar-belakang"></a>Latar Belakang

Dragonfly berawal dari eksperimen untuk menjawab pertanyaan: *seperti apa arsitektur sistem in-memory datastore jika dirancang ulang dari nol pada tahun 2022?* Berangkat dari pengalaman kami sebagai pengguna setia memory datastore dan insinyur di berbagai perusahaan cloud computing, ada dua pilar utama yang mutlak dipertahankan untuk Dragonfly: **jaminan sifat atomik (*atomicity*) pada setiap operasi** dan **latensi sub-milidetik yang konsisten pada volume throughput ultra-tinggi**.

Tantangan pertama adalah bagaimana memaksimalkan utilisasi CPU, memori, dan kapabilitas I/O pada perangkat keras server modern yang tersedia di platform cloud saat ini. Untuk mengatasinya, kami menerapkan arsitektur [shared-nothing](https://en.wikipedia.org/wiki/Shared-nothing_architecture). Arsitektur ini mempartisi ruang kunci (*keyspace*) ke berbagai thread kerja, sehingga masing-masing thread mengelola irisan kamus datanya sendiri secara independen tanpa saling berebut kunci. Irisan ini kami sebut sebagai "shard". Pustaka (*library*) yang menggerakkan manajemen thread dan I/O performa tinggi ini telah kami buka sebagai proyek open-source di [Helio](https://github.com/romange/helio).

Untuk memberikan jaminan transaksi atomik pada operasi multi-kunci (*multi-key*), kami mengadopsi hasil riset akademis terkini: paper ["VLL: a lock manager redesign for main memory database systems"](https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf). Kombinasi arsitektur *shared-nothing* dan konsep VLL memungkinkan Dragonfly mengeksekusi operasi transaksi atomik multi-kunci tanpa memerlukan *mutex* tradisional atau *spinlock*. Ini menjadi terobosan penting yang melipatgandakan performa Dragonfly melampaui solusi sejenis lainnya.

Tantangan kedua adalah merancang struktur data internal yang lebih efisien. Struktur tabel hash utama Dragonfly dikembangkan berdasarkan riset paper ["Dash: Scalable Hashing on Persistent Memory"](https://arxiv.org/pdf/2003.07302.pdf). Desain *hashtable* Dash memungkinkan kami mempertahankan dua keunggulan krusial milik Redis: kemampuan *incremental hashing* saat datastore bertumbuh dinamis dan kemampuan menelusuri kamus data di tengah perubahan data menggunakan operasi *stateless scan*. Di luar itu, Dash jauh lebih hemat konsumsi CPU dan memori. Berkat fondasi Dash, kami berhasil menghadirkan inovasi lanjutan:
* Pembersihan masa kedaluwarsa (*record expiry*) yang efisien untuk data bertipe TTL.
* Algoritma *cache eviction* baru yang menghasilkan tingkat *cache hit* lebih tinggi daripada strategi LRU atau LFU standar, dengan **overhead memori sebesar nol**.
* Algoritma pembuatan snapshot yang bekerja **tanpa proses `fork()` (*fork-less*)**.

Setelah fondasi utama Dragonfly terbukti tangguh dan [mencapai target performa tinggi](#tolok-ukur-kinerja-benchmarks), kami melengkapi fungsionalitas kompatibilitas perintah Redis dan Memcached. Hingga kini, Dragonfly telah mengimplementasikan sekitar 185 perintah Redis (setara dengan fungsionalitas penuh Redis 5.0) dan 13 perintah Memcached.

Dan akhirnya, <br>
<em>Misi kami adalah menghadirkan in-memory datastore yang dirancang indah, berkecepatan tinggi, dan hemat biaya untuk beban kerja cloud modern dengan memanfaatkan kemajuan perangkat keras terkini—menyelesaikan kelemahan solusi yang ada saat ini tanpa mengubah API dan kemudahan integrasinya.</em>

## <a name="kontributor"></a>Kontributor

Terima kasih yang sebesar-besarnya kepada seluruh kontributor proyek Dragonfly!

<a href="https://github.com/dragonflydb/dragonfly/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=dragonflydb/dragonfly" />
</a>
