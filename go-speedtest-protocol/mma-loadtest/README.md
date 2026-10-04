# mma-loadtest (Go)

Go-перенос нагрузочного клиента MMA с тем же бинарным framing, что и исходная Rust-версия.

## Требования

- Go 1.23+
- MMA server с тем же `FramerConfig`

Зависимость `github.com/HdrHistogram/hdrhistogram-go` используется для HDR-распределения latency; v1.3.0 требует Go 1.23.

## Установка

```bash
go mod tidy
go build -trimpath -ldflags="-s -w" -o mma-loadtest .
```

## Closed-loop

```bash
./mma-loadtest 127.0.0.1:8080 --concurrency 50 --requests 20000
```

На каждом TCP-соединении: `request -> response -> следующий request`.

## Open-loop

```bash
./mma-loadtest 127.0.0.1:8080 --concurrency 50 --requests 20000 --rate 500
```

`--rate 500` означает 500 запросов/сек **на одно соединение**.

Расписание строится по абсолютному времени. Если отправка отстала от графика, следующий запрос не ждёт дополнительный тик — это воспроизводит семантику Tokio `MissedTickBehavior::Burst`.

## Аргументы

```text
--concurrency N   параллельных TCP-соединений, default 50
--requests N      измеряемых запросов на соединение, default 2000
--warmup N        прогревочных запросов на соединение, default 100
--route STR       маршрут, default GET
--payload STR     payload, default "hello world, this is a small payload"
--rate N          open-loop, запросов/сек на соединение; без него closed-loop
```

## Что сохранено

- ручная сборка 29-байтового заголовка;
- `req_id` в последних 4 байтах заголовка;
- warmup с `0x80000000` marker;
- `TCP_NODELAY`;
- общий старт измерительной фазы после connect + warmup;
- out-of-order ответы в open-loop через `req_id`;
- 5 секунд drain grace period;
- HDR: min / p50 / p90 / p95 / p99 / p99.9 / max / mean / stdev.

Go `net.Conn` допускает одновременные вызовы `Read` и `Write` из разных goroutine, поэтому open-loop использует отдельную reader goroutine и writer в основной worker goroutine. citeturn441311search1
