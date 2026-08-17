## Full-sync fanout benchmark

Configuration: 4 GiB random data, one proactor per instance, continuous 1 KiB `SET` and `GET` load, and a 1-second fanout collection window.

| Replicas | Without fanout | With fanout | Result |
| ---: | ---: | ---: | --- |
| 1 | 4.13 s | 4.04 s | 2.2% faster |
| 2 | 6.24 s | 6.75 s | 8.2% slower |
| 3 | 9.47 s | 7.76 s | 18.1% faster (1.22×) |
| 4 | 13.40 s | 9.53 s | 28.9% faster (1.41×) |
| 5 | 19.20 s | 12.20 s | 36.5% faster (1.57×) |

Fanout starts to improve full-sync time at three replicas with this collection-window setting.
