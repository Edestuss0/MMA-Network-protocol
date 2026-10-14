# MMA Protocol: структурированный аудит проекта

Дата отчёта: 2026-10-11  
Базовый коммит аудита: `34f2542`  
Язык отчёта: русский

> В рабочем дереве есть незакоммиченные изменения в ключевых файлах. Поэтому в отчёте отдельно указаны свойства базового коммита и факты, подтверждённые текущим деревом.

## 1. Резюме

MMA — закрытый бинарный протокол поверх TCP для собственного мессенджера. Протокол использует конфигурируемое кадрирование, режим request/response `Once`, мультиплексирование по `req_id`, маршрутизацию по строковому route и Rust-ядро с частичным C ABI.

Текущая реализация функционально работает, но остаётся прототипом. Главные риски находятся не в скорости кодека, а в отказоустойчивости сервера, жизненном цикле C API, отсутствии тестов, отсутствии лимитов соединений и невозможности безопасно добавить `Channel` без расширения wire format.

**Итоговая оценка готовности req/res к первому pre-release: 6/10.**

До pre-release необходимо закрыть `S-01`–`S-06`, заморозить wire format, описать его в README и подтвердить поведение нагрузочными и отказоустойчивыми тестами.

## 2. Идея и назначение протокола

### 2.1. Основная концепция

- MMA предназначен для закрытой экосистемы собственного мессенджера.
- Совместимость с внешними протоколами не является целью.
- Клиент и сервер должны использовать одинаковый `FramerConfig`.
- Один TCP-коннект поддерживает несколько одновременных запросов.
- Ответ сопоставляется с запросом по `req_id`, поэтому ответы могут приходить не по порядку.
- Формат кадра отделён от транспортной защиты: шифрование предполагается через TLS поверх `Server::from_listener`.

### 2.2. Реализованные режимы

| Режим | Состояние | Назначение |
|---|---|---|
| `Once` | Работает | Один запрос — один ответ |
| `Channel` | Не реализован | Долгоживущий двунаправленный канал |
| Динамическая смена конфига | Не реализована | Согласованная смена wire format |
| Маршрутизация каналов по соединениям | Не реализована | Разделение каналов по TCP-коннектам |

## 3. Состав проекта

| Файл | Ответственность |
|---|---|
| `src/protocol/framer.rs` | Кодирование и декодирование кадров |
| `src/core/config.rs` | `Config`, `FramerConfig`, версия протокола и валидация |
| `src/core/commands.rs` | Request/Response, заголовки, opcode, ошибки |
| `src/core/router.rs` | Синхронная маршрутизация по route |
| `src/core/server.rs` | Accept loop, соединения, writer, shutdown |
| `src/c_api.rs` | Серверный C ABI |
| `include/mma.h` | Публичные C-структуры и функции |
| `examples/main_server.rs` | Rust-пример |
| `examples/main_server.c` | C-пример |
| `MMA-client` | Каталог под клиентскую часть; полноценного клиента пока нет |

## 4. Wire format

### 4.1. Заголовок

Минимальный заголовок имеет длину 29 байт:

```text
fixed header (header_length bytes)
body (payload + options + route)
```

Все числа кодируются в big-endian. Позиции полей в заголовке задаются 1-based индексами `FramerConfig`.

| Поле | Размер | Назначение |
|---|---:|---|
| `opcode` | 1 байт | Тип запроса или ответа |
| `version` | 1 байт | Версия MMA, сейчас `1` |
| `route_len` | 1 байт | Длина route |
| `options_count` | 1 байт | Число опций |
| резерв | переменный | Неиспользуемая часть заголовка |
| `options_len` | 4 байта | Размер секции options |
| `payload_len` | 4 байта | Размер payload |
| `req_id` | 4 байта | Идентификатор запроса |

Reserved suffix всегда находится в конце заголовка и сейчас занимает 12 байт:

```text
[options_len: u32][payload_len: u32][req_id: u32]
```

### 4.2. Тело кадра

Тело состоит из трёх секций:

- `payload`;
- `options`;
- `route`.

Порядок задаётся полями `payload_order`, `options_order`, `route_order`. Значения должны быть перестановкой `1`, `2`, `3`.

### 4.3. Опции

Каждая опция имеет формат:

```text
[key_len: u16][value_len: u16][key][value]
```

Если `options_key_first == false`, секция данных имеет порядок `[value][key]`.

Ограничения:

- максимум 255 опций;
- ключ и значение непустые;
- размер ключа и значения — до 65535 байт;
- ключ и значение должны быть UTF-8;
- общий размер тела не должен превышать `max_message_length`.

### 4.4. Opcode

Запросы:

| Значение | Opcode |
|---:|---|
| 1 | `Channel` |
| 2 | `Once` |

Ответы:

| Значение | Opcode |
|---:|---|
| 1 | `Ok` |
| 2 | `BadRequest` |
| 3 | `Unauthorized` |
| 4 | `Forbidden` |
| 5 | `NotFound` |
| 6 | `Conflict` |
| 7 | `InternalError` |
| 8 | `Message` |
| 9 | `Shutdown` |

После заморозки формата значения opcode нельзя менять.

## 5. Анализ реализации

### 5.1. Framer

Сильные стороны:

- поддержка фрагментированных TCP-кадров;
- кэширование разобранного заголовка;
- работа через `Bytes`/`BytesMut`;
- откат буфера при ошибке encode;
- конфигурируемый порядок секций;
- проверки длины тела и переполнения при декодировании.

Проблемы:

- `route` и options выделяются как новые `String`;
- итоговый размер тела проверяется после записи части тела;
- требуется `checked_add` во всех операциях вычисления размера;
- ошибка протокола закрывает соединение без гарантированного `BadRequest`;
- `ResponseOpcode` дублируется в C ABI;
- часть структуры кода не проходит строгий Clippy.

### 5.2. Router

Текущий handler:

```rust
type Handler = Arc<dyn Fn(Request, &mut Response) + Send + Sync>;
```

Плюсы:

- быстрый lookup через `AHashMap`;
- ответы сохраняют исходный `req_id`;
- неизвестный route получает `NotFound`;
- неправильная версия получает `BadRequest`.

Минус: handler синхронный. Блокирующая операция занимает Tokio worker и может задержать другие запросы.

### 5.3. Server

Текущая схема:

```text
accept loop
  └── serve_connection
       ├── reader
       ├── writer
       └── inflight handlers
```

Реализовано:

- `TCP_NODELAY`;
- отдельный writer на соединение;
- очередь ответов через `mpsc`;
- batching через `max_batch`;
- глобальный лимит обработчиков через semaphore;
- `CancellationToken`;
- `TaskTracker`;
- graceful shutdown;
- `Server::from_listener` для TLS и тестов.

Проблемы:

1. Глобальный semaphore permit удерживается до `tx.send(response)`.
2. Медленный клиент может занять все глобальные слоты.
3. Паника обработчика не превращается в `InternalError`.
4. Reader не получает отдельный сигнал о падении writer.
5. Нет лимита количества TCP-соединений.
6. Нет idle timeout.
7. Нет write timeout.
8. Ошибка `accept()` завершает сервер.
9. Graceful shutdown может ждать drain timeout.

### 5.4. C ABI

Предоставлены функции создания сервера, регистрации route, запуска и формирования ответа. Клиентского и codec C API нет.

Дефекты текущей реализации:

- `mma_server_start()` вызывает `server.run()` и тем самым перехватывает `Ctrl+C`;
- bind выполняется в отдельном потоке после возврата из `start()`;
- ошибка bind не возвращается вызывающему коду;
- `started` не переводится в `true`;
- повторный запуск возможен;
- фактический порт при `port = 0` не возвращается;
- `mma_server_stop()` и `mma_server_destroy()` закомментированы;
- panic в callback может пересечь FFI boundary;
- блокирующий callback занимает Tokio worker;
- `Shutdown = 9` отсутствует в C-заголовке и таблице `response_opcode()`;
- C-пример использует busy loop.

## 6. Реестр проблем

| ID | Приоритет | Проблема | Последствие |
|---|---|---|---|
| P-01 | Критично | Permit удерживается до отправки ответа | Один медленный клиент блокирует сервер |
| P-02 | Высоко | Panic handler не обрабатывается | Клиент ждёт ответа бесконечно |
| P-03 | Высоко | Плохой opcode/UTF-8 закрывает соединение молча | Слабая диагностика и несовместимое поведение |
| P-04 | Высоко | Нет лимита соединений и idle timeout | DoS по памяти и task slots |
| P-05 | Средне | Reader не знает о падении writer | Zombie connection |
| P-06 | Средне | Любая ошибка accept останавливает сервер | Потеря доступности |
| P-07 | Высоко | Неполный lifecycle C ABI | Нельзя корректно остановить и освободить сервер |
| P-08 | Средне | C callback не защищён от panic/blocking | Падение процесса или starvation |
| P-09 | Средне | Shutdown не завершён во всех слоях | Неполный протокол остановки |
| P-10 | Средне | Opcode дублируется в Rust и C | Риск рассинхронизации |
| P-11 | Высоко | Нет тестов и CI | Регрессии не контролируются |
| P-12 | Низко | Лишний payload в `Response::new()` | Неожиданное поведение API |
| P-13 | Средне | Синхронные handlers | Блокировка worker threads |
| P-14 | Низко | Лишние String allocations | Рост latency и нагрузки allocator |
| P-15 | Низко | Shutdown ждёт drain timeout | Остановка занимает до 1 секунды |
| P-16 | Средне | Нет README/specification | Клиентов нельзя реализовывать надёжно |
| P-17 | Высоко, дизайн | Нет `stream_id` и `flags` | Channel требует изменения wire format |

## 7. Производительность

### 7.1. Известные измерения

По методике аудита:

- Linux;
- 2 общих ядра;
- `cargo build --release`;
- клиент и сервер на одной машине;
- closed-loop;
- прогрев 100 запросов на соединение;
- payload 36 байт;
- route `GET`.

Результаты прошлых прогонов:

| Режим | Throughput | p50 | p99 |
|---|---:|---:|---:|
| 1 соединение | 25 121 req/s | 38 мкс | 81 мкс |
| 50 соединений | 100 813 req/s | 454 мкс | 1 364 мкс |
| 200 соединений | 123 857 req/s | 1 127 мкс | 6 119 мкс |

Ориентировочная скорость кодека:

- decode: около 140 нс;
- encode: около 78 нс.

Разброс на общем двухъядерном хосте достигает ±20%. Эти цифры нельзя сравнивать с HTTP на другой машине и при другой методике.

### 7.2. Реальные bottleneck

Основные расходы находятся в:

1. Планировании Tokio task.
2. Создании task на каждый запрос.
3. `String` allocation для route/options.
4. Ожидании writer.
5. Глобальной конкуренции за semaphore.
6. Блокирующих handlers.
7. Очереди `mpsc(1000)` на соединение.

Оптимизировать кодек следует после устранения проблем корректности и back-pressure.

### 7.3. Что измерять перед release

- req/s;
- p50, p95, p99 latency;
- peak RSS;
- allocations per request;
- время ожидания semaphore;
- время ожидания writer;
- размер очереди ответов;
- число соединений;
- поведение при медленном читателе;
- поведение при медленном handler;
- payload от маленького до максимального.

## 8. План доведения до pre-release

### S-01. Защита сервера — P0

Нужно:

- добавить semaphore на соединение, например 64 запроса;
- отпускать глобальный permit до `tx.send`;
- оборачивать handler в `catch_unwind`;
- добавить write timeout 10 секунд;
- добавить idle timeout 120 секунд;
- передавать cancellation от writer к reader;
- закрывать reader при падении writer.

Пример правильного порядка:

```rust
inflight.spawn(async move {
    let response = match std::panic::catch_unwind(
        std::panic::AssertUnwindSafe(|| router.handle(request)),
    ) {
        Ok(response) => response,
        Err(_) => Response::internal_error(req_id),
    };

    drop(permit);

    let _ = tx.send(response).await;
});
```

Критерий: медленный читатель одного соединения не задерживает быстрый запрос другого соединения.

### S-02. Ошибки протокола — P0

При ошибке после разбора заголовка нужно отправить `BadRequest`, используя распознанный `req_id`. Если заголовок разобрать нельзя, используется `req_id = 0`, после чего соединение закрывается.

```rust
Err(error) => {
    let response = Response::bad_request(0, error.to_string());
    let _ = tx.send(response).await;
    break;
}
```

### S-03. Лимиты — P0

Добавить в `Config`:

```rust
pub max_connections: usize,
pub idle_timeout: Duration,
pub write_timeout: Duration,
```

При достижении лимита соединение закрывается сразу:

```rust
let permit = match connection_limit.clone().try_acquire_owned() {
    Ok(permit) => permit,
    Err(_) => {
        let _ = socket.shutdown().await;
        continue;
    }
};
```

Временная ошибка `accept()` не должна завершать accept loop.

### S-04. C ABI — P0

Реализовать:

- ожидание успешного bind внутри `mma_server_start()`;
- сохранение `JoinHandle` и shutdown token;
- `mma_server_stop()`;
- `mma_server_destroy()`;
- отсутствие `ctrl_c` внутри библиотеки;
- `catch_unwind` вокруг callback;
- изоляцию блокирующих callback;
- реальный порт при `port = 0`;
- `MMA_RESPONSE_SHUTDOWN` в `include/mma.h`;
- отсутствие busy loop в C-примере.

Пример остановки:

```rust
let Some(running) = server.running.take() else {
    return MMA_OK;
};

running.stop.cancel();

if running.thread.join().is_err() {
    return MMA_SERVER_ERROR;
}

MMA_OK
```

### S-05. Единый источник opcode — P0

Добавить `TryFrom<u8>` для `ResponseOpcode` и использовать его из C API. Таблица значений должна находиться в одном месте.

### S-06. Тесты и CI — P0

Минимальный набор тестов:

- round-trip кадров;
- фрагментация по одному байту;
- фрагментация заголовка и тела;
- все варианты порядка секций;
- `options_key_first = false`;
- invalid opcode;
- invalid UTF-8 route/options;
- oversized payload/options;
- integer overflow;
- panic handler;
- slow reader;
- slow handler;
- connection limit;
- graceful shutdown;
- C ABI lifecycle.

CI должен запускать:

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
cargo build --release
```

### S-07. Гигиена кодека — P1

Заменить `Result<(), ()>` на специальный `ConfigError`, добавить `Default` для `FramerConfig` и `Router`, использовать `checked_add`, убрать лишний payload из `Response::new()` и проверять лимит тела до записи в буфер.

### S-08. Shutdown frame — P1

Нужно зафиксировать, должен ли сервер отправлять перед FIN:

```text
opcode = Shutdown (9)
req_id = 0
```

Решение должно войти в спецификацию и тесты.

### S-09. Async handlers — P1

Для долгоживущих операций нужен асинхронный handler API:

```rust
type AsyncHandler = Arc<
    dyn Fn(Request) -> BoxFuture<'static, Response>
        + Send
        + Sync
>;
```

Блокирующие операции следует направлять через `spawn_blocking`.

### S-10. Снижение аллокаций — P2

Последовательность:

1. Искать route по `&str`.
2. Хранить route как `Arc<str>` или `Bytes`.
3. Представлять options байтовыми срезами.
4. Использовать `SmallVec` для типичных небольших списков.
5. Измерить allocations/request до и после изменений.

### S-11. Настраиваемые таймауты — P2

Вынести `GRACE`, `DRAIN`, `IDLE` и `WRITE` из констант в `Config` и C API.

### S-12. README и спецификация — P1

README должен описывать назначение, кадр, дефолтный конфиг, opcode, ограничения, ошибки, lifecycle, security model и статус Channel.

## 9. Переход к Channel

### 9.1. Когда начинать

`Channel` нельзя начинать до закрытия:

```text
S-01, S-02, S-03, S-04, S-05, S-06
```

Также должны быть выполнены условия:

- wire format заморожен;
- написана спецификация;
- исправлен back-pressure;
- определены timeout и правила ошибок;
- есть Rust-клиент;
- async handlers готовы;
- нагрузочный прогон стабилен;
- lifecycle соединений покрыт тестами.

### 9.2. Необходимое расширение wire format

Текущий заголовок не содержит `stream_id` и `flags`. Для Channel нужно зарезервировать:

```text
[options_len: u32]
[payload_len: u32]
[req_id: u32]
[stream_id: u32]
[flags: u8]
```

Это увеличивает reserved suffix минимум с 12 до 17 байт. Точный размер и расположение нужно принять до заморозки первого релиза.

Для режима `Once` рекомендуется `stream_id = 0`.

### 9.3. Предлагаемые opcode Channel

| Значение | Opcode |
|---:|---|
| 1 | `ChannelOpen` |
| 2 | `Once` |
| 3 | `ChannelData` |
| 4 | `ChannelClose` |
| 5 | `Ping` |
| 6 | `Pong` |
| 7 | `WindowUpdate` |

### 9.4. Минимальная серверная модель

```rust
struct ChannelState {
    id: u32,
    outbound: tokio::sync::mpsc::Sender<Response>,
    window: u32,
    close: tokio_util::sync::CancellationToken,
}
```

На соединение потребуется таблица:

```rust
HashMap<u32, ChannelState>
```

Необходимые механизмы:

- открытие и закрытие канала;
- передача данных;
- server push;
- кредитное окно и `WindowUpdate`;
- per-channel back-pressure;
- Ping/Pong;
- child cancellation token;
- закрытие всех каналов при остановке сервера;
- клиентская библиотека для тестирования.

## 10. Инварианты

| ID | Инвариант |
|---|---|
| I-01 | Позиции полей заголовка 1-based, числа big-endian |
| I-02 | Reserved suffix всегда в конце заголовка |
| I-03 | Ответ содержит `req_id` соответствующего запроса |
| I-04 | Одно соединение не блокирует другие |
| I-05 | Обработчики завершаются до writer и FIN |
| I-06 | Значения opcode не меняются после freeze wire format |
| I-07 | Panic не пересекает FFI boundary |
| I-08 | C ABI не перехватывает сигналы процесса |

## 11. Открытые решения

До заморозки формата необходимо решить:

1. Как проверять совпадение конфигураций: hash, epoch или handshake.
2. Какие платформы являются первыми целевыми.
3. На каком языке выпускать первую клиентскую библиотеку.
4. Где находятся authorization и session management: в ядре или в мессенджере.
5. Каковы реальные лимиты размера сообщения и количества соединений.
6. Нужен ли `Shutdown` frame перед FIN.
7. Какой точный формат `stream_id` и `flags` будет заморожен.

## 12. Финальная оценка

| Аспект | Оценка |
|---|---:|
| Идея протокола | 8/10 |
| Формат кадра | 7/10 |
| Корректность | 7/10 |
| Производительность codec | 8/10 |
| End-to-end производительность | 7/10 |
| Отказоустойчивость | 4/10 |
| Безопасность и DoS-защита | 3/10 |
| Память | 5/10 |
| Архитектура | 7/10 |
| Rust API | 6/10 |
| C ABI | 4/10 |
| Тесты и CI | 0/10 |
| Документация | 2/10 |
| Pre-release readiness | 6/10 |

Рекомендуемая последовательность:

```text
server hardening
→ C lifecycle
→ единый список opcode
→ тесты и CI
→ wire freeze
→ README/specification
→ async handlers
→ Rust client
→ Channel
```
