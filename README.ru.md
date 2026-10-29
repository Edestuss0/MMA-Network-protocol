<div align="center">

# MMA Network Protocol

**Бинарный TCP-протокол с настраиваемым wire-форматом — ядро на Rust, C ABI.**

[English](README.md) · **Русский**

</div>

> **Статус:** pre-release `0.1.0` · версия wire-протокола `1` · API и C ABI могут меняться без предупреждения.

MMA — приватный бинарный протокол поверх TCP с режимами запрос/ответ и channel. Главная особенность: **единого фиксированного wire-формата нет.** Раскладка байтов кадра определяется конфигурацией (`FramerConfig`), общей для обеих сторон, поэтому два развёртывания с разными конфигурациями выглядят на проводе как два разных несовместимых протокола.

Документ описывает формат кадра, модель соединения, Rust API и C ABI.

## Содержание

- [1. Обзор](#1-обзор)
- [2. Быстрый старт](#2-быстрый-старт)
- [3. Формат кадра](#3-формат-кадра)
- [4. Options](#4-options)
- [5. Маршруты и модель соединения](#5-маршруты-и-модель-соединения)
- [6. Ошибки](#6-ошибки)
- [7. Rust API](#7-rust-api)
- [8. C ABI](#8-c-abi)
- [9. Совместимость и версионирование](#9-совместимость-и-версионирование)
- [10. Безопасность](#10-безопасность)
- [11. Статус и известные ограничения](#11-статус-и-известные-ограничения)
- [12. Структура репозитория](#12-структура-репозитория)
- [13. Go-обёртка без CGO](#13-go-обёртка-без-cgo)

---

## 1. Обзор

### 1.1. Идея: wire-формат без единого стандарта

У большинства протоколов (HTTP, RESP, MQTT, gRPC) раскладка байтов зафиксирована спецификацией, поэтому любой сниффер опознаёт их по сигнатуре. MMA задаёт **правила, а не раскладку**. Раскладка выбирается для каждого развёртывания:

| Поле `FramerConfig` | Что задаёт |
|---|---|
| `header_length` | размер заголовка, 17–255 байт |
| `opcode_pos`, `version_pos`, `route_len_pos`, `options_count_pos` | в каком байте заголовка лежит каждое однобайтовое поле |
| `big_bytes_config` | какой из трёх 4-байтовых слотов в конце заголовка хранит `options_len`, `payload_len`, `req_id` |
| `route_order`, `options_order`, `payload_order` | порядок секций тела (перестановка 1..3) |
| `options_key_first` | порядок key/value внутри option |
| spare-байты заголовка | нули при encode, игнорируются при decode |

Следствия:

- В кадре нет постоянной сигнатуры (magic number); позиции полей и порядок секций различаются между развёртываниями. Это усложняет распознавание по простому шаблону, но это **обфускация, а не механизм безопасности**.
- Конфигурация — **общий wire-профиль**, который клиент и сервер согласуют вне протокола. Значения на обеих сторонах должны совпадать.
- Неизменна **логическая модель**: `opcode`, `version`, `req_id`, `route`, `options`, `payload`.
- MMA **не шифрует и не аутентифицирует**. Используйте TLS или доверенную сеть (см. [Безопасность](#10-безопасность)).
- Рукопожатия пока нет: при разных конфигурациях сервер неверно разбирает кадры — обычно отвечает `BadRequest` и закрывает соединение.

### 1.2. Назначение

MMA предназначен для связи собственных клиентов и серверов, когда нужны:

- компактный кадр с явными длинами;
- wire-формат, настраиваемый под развёртывание;
- два режима — **Once** (запрос → один ответ) и **Channel** (постоянное соединение с push от сервера);
- один серверный движок для Rust и C.

Вне задач протокола: шифрование, аутентификация, service discovery, совместимость со сторонними клиентами, гарантии доставки между перезапусками. Клиент в crate **не входит** — пишите его по этой спецификации.

### 1.3. Свойства

| Свойство | Значение |
|---|---|
| Транспорт | TCP |
| Взаимодействие | Request/Response (`Once`) и `Channel` |
| Кодирование | бинарное, раскладка задаётся `FramerConfig` |
| Версия wire | `MMA_VERSION = 1` |
| Размер заголовка | минимум 17 байт, по умолчанию 29 |
| Максимальный кадр | `header_length + max_message_length` (по умолчанию `29 + 10 000 000`) |
| Порядок байтов | big-endian |
| Интерфейсы | Rust (серверный API), C (`include/mma.h`) |

---

## 2. Быстрый старт

### Rust-сервер

```toml
[dependencies]
MMA = { git = "https://github.com/Edestuss0/MMA-Network-protocol" }
tokio = { version = "1", features = ["full"] }
bytes = "1"
```

```rust
use MMA::core::{commands::{Request, Response}, config::Config,
                router::Router, server::Server};
use bytes::Bytes;

async fn upper(req: Request, mut res: Response) -> Response {
    res.payload = Bytes::from(req.payload.to_ascii_uppercase());
    res
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut server = Server::new(Config::default()).await?;
    let mut router = Router::new();
    router.register_route("UP".to_string(), upper).expect("route");
    server.run(router).await
}
```

Готовый пример: `cargo run --example main_server`.

### Минимальный клиент (Python, раскладка по умолчанию)

```python
import socket, struct

HEADER_LEN = 29

def recv_exact(sock, size):
    buf = bytearray()
    while len(buf) < size:
        part = sock.recv(size - len(buf))
        if not part:
            raise EOFError("connection closed")
        buf.extend(part)
    return bytes(buf)

def make_frame(opcode, route, payload=b"", req_id=1):
    h = bytearray(HEADER_LEN)
    h[0], h[1], h[2], h[3] = opcode, 1, len(route), 0   # opcode, version, route_len, options_count
    h[17:29] = struct.pack(">III", 0, len(payload), req_id)  # options_len, payload_len, req_id
    return bytes(h) + route + payload

with socket.create_connection(("127.0.0.1", 8080)) as s:
    s.sendall(make_frame(2, b"UP", b"hello", req_id=1))
    h = recv_exact(s, HEADER_LEN)
    payload_len = struct.unpack(">I", h[21:25])[0]
    print(h[0], recv_exact(s, payload_len))   # 1 b'HELLO'
```

### C-сервер

```bash
cargo build --release
gcc -std=c11 -Wall -Wextra -pthread -Iinclude examples/main_server.c \
    -Ltarget/release -lMMA -Wl,-rpath,"$(pwd)/target/release" -o mma_server_c
```

См. [8. C ABI](#8-c-abi).

---

## 3. Формат кадра

### 3.1. Схема

```text
┌──────────────────────────────────────────────────────────────┐
│ Header — header_length байт (≥ 17, по умолчанию 29)          │
│   opcode / version / route_len / options_count               │
│       (по одному байту, на настроенных позициях)             │
│   spare-байты (нули)                                         │
│   последние 12 байт: три 4-байтовых big-endian слота с       │
│       options_len, payload_len, req_id                       │
│       (порядок слотов задаёт big_bytes_config)               │
├──────────────────────────────────────────────────────────────┤
│ Body — три секции в порядке route_order / options_order /    │
│        payload_order                                         │
│   Route    route_len байт (UTF-8)                            │
│   Options  options_len байт                                  │
│   Payload  payload_len байт                                  │
└──────────────────────────────────────────────────────────────┘
```

В ответах секции route нет (`route_len = 0`).

### 3.2. Поля заголовка

Смещения — от начала кадра, с нуля; `H = header_length`. Позиции `*_pos` в конфигурации считаются с единицы, поэтому смещение = `pos − 1`. Смещения трёх 4-байтовых полей указаны для порядка слотов по умолчанию.

| Поле | Смещение | Размер | Тип | Назначение | Проверка |
|---|---:|---:|---|---|---|
| `opcode` | `opcode_pos − 1` (по ум. 0) | 1 | `u8` | Тип запроса или ответа ([3.4](#34-opcode)) | запрос: 1, 2 или 3 |
| `version` | `version_pos − 1` (по ум. 1) | 1 | `u8` | Версия протокола, сейчас `1` | проверяет роутер |
| `route_len` | `route_len_pos − 1` (по ум. 2) | 1 | `u8` | Длина route; в ответах `0` | 0–255, route — UTF-8 |
| `options_count` | `options_count_pos − 1` (по ум. 3) | 1 | `u8` | Число options | 0–255, совпадает с разобранным |
| spare | остальное в `[0, H−12)` | `H − 16` | — | Резерв для вариации раскладки | нули при encode, игнорируются при decode |
| `options_len` | `H − 12` (по ум.) | 4 | `u32` BE | Размер секции options | входит в `max_message_length` |
| `payload_len` | `H − 8` (по ум.) | 4 | `u32` BE | Размер payload | входит в `max_message_length` |
| `req_id` | `H − 4` (по ум.) | 4 | `u32` BE | Идентификатор запроса, возвращается в ответе | любое `u32` |

Зачем нужно каждое поле:

- **`opcode`** — что это за кадр и как его обрабатывать.
- **`version`** — версия протокола; позволяет менять семантику без смены раскладки.
- **`route_len`** — граница секции route; route выбирает обработчик на сервере.
- **`options_count`** — число пар key/value; вместе с `options_len` проверяет целостность секции.
- **`options_len`, `payload_len`** — размеры секций; по ним вычисляется длина кадра.
- **`req_id`** — связывает ответ с запросом. Ответы могут приходить не по порядку, поэтому `req_id` среди запросов в полёте должны быть уникальны.
- **spare-байты** — свободное место заголовка для вариации раскладки; смысла не несут.

### 3.3. Длина кадра

```text
body_len  = route_len + options_len + payload_len    (checked-сложение)
frame_len = header_length + body_len                 (checked-сложение)
```

`body_len` сравнивается с `max_message_length` до любой аллокации. Кадр может прийти за несколько чтений TCP, несколько кадров — за одно; границы определяются только полями длины.

### 3.4. Opcode

**Запросы (клиент → сервер)**

| Название | Код | Допустимо в состоянии | Назначение |
|---|---:|---|---|
| `ChannelConnect` | 1 | `Fresh` | Перевести соединение в режим Channel |
| `Once` | 2 | `Fresh`, `Once` | Запрос обрабатывается route-обработчиком; один ответ |
| `ChannelMessage` | 3 | `Channel` | Сообщение channel-обработчику; при успехе ответа нет |

**Ответы (сервер → клиент)**

| Название | Код | Значение | Когда отправляется |
|---|---:|---|---|
| `Ok` | 1 | Успех | Статус ответа по умолчанию |
| `BadRequest` | 2 | Некорректный запрос | Ошибка декодирования; неверная версия; channel-сообщение без канала |
| `Unauthorized` | 3 | Не авторизован | Выставляет ваш обработчик |
| `Forbidden` | 4 | Запрещено | Выставляет ваш обработчик |
| `NotFound` | 5 | Route не найден | payload `"No such method"` |
| `Conflict` | 6 | Конфликт состояния | Повторный `ChannelConnect` |
| `InternalError` | 7 | Ошибка сервера | Panic обработчика |
| `ChannelMessage` | 8 | Push от сервера | Доставка через `send_to_channel` |
| `Shutdown` | 9 | Сервер останавливается | Зарезервирован; сейчас не отправляется |
| `ChannelConnected` | 10 | Канал установлен | Ответ на `ChannelConnect` |
| `Timeout` | 11 | Таймаут обработчика | Истёк `request_timeout` |

Коды `0` и `≥ 12` не определены. Неизвестный opcode запроса — ошибка декодирования.

### 3.5. Примеры кадров

Раскладка по умолчанию (`header_length = 29`, позиции 1/2/3/4, порядок route → options → payload).

Запрос: `Once`, route `GET`, payload `hello`, `req_id = 1` — 37 байт:

```text
02 01 03 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 05 00 00 00 01 47 45 54 68 65 6c 6c 6f
```

| Смещение | Hex | Поле | Значение |
|---:|---|---|---|
| 0 | `02` | `opcode` | `Once` |
| 1 | `01` | `version` | 1 |
| 2 | `03` | `route_len` | 3 |
| 3 | `00` | `options_count` | 0 |
| 4–16 | `00 …` | spare | 13 нулевых байт |
| 17–20 | `00 00 00 00` | `options_len` | 0 |
| 21–24 | `00 00 00 05` | `payload_len` | 5 |
| 25–28 | `00 00 00 01` | `req_id` | 1 |
| 29–31 | `47 45 54` | route | `"GET"` |
| 32–36 | `68 65 6c 6c 6f` | payload | `"hello"` |

Ответ `Ok` — 34 байта:

```text
01 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 05 00 00 00 01 68 65 6c 6c 6f
```

Тот же логический запрос при другой конфигурации (например, `version_pos=4`, `route_len_pos=6`, `options_count_pos=7`, порядок payload → options → route, как в `examples/main_server.rs`) даст совершенно другие байты. В этом и состоит идея.

### 3.6. Конфигурация раскладки

| Поле | Допустимые значения | По умолчанию | Влияние на wire |
|---|---|---|---|
| `header_length` | 17–255 | 29 | размер заголовка, положение 12-байтового хвоста |
| `opcode_pos`, `version_pos`, `route_len_pos`, `options_count_pos` | 1…`header_length − 12`, попарно различны | 1, 2, 3, 4 | смещения однобайтовых полей |
| `route_order`, `options_order`, `payload_order` | перестановка 1..3 | 1, 2, 3 | порядок секций тела |
| `big_bytes_config.options_len_order` / `payload_len_order` / `req_id_order` | перестановка 1..3 | 1, 2, 3 | слот каждого 4-байтового поля в хвосте |
| `options_key_first` | `true` / `false` | `true` | `key,value` или `value,key` внутри option |
| `max_message_length` | `u32` | 10 000 000 | не параметр раскладки; серверный лимит размера |

Все поля, кроме `max_message_length`, — параметры wire-формата и **должны совпадать на обеих сторонах**.

### 3.7. Неполные и некорректные кадры

| Ситуация | Реакция сервера | Соединение |
|---|---|---|
| Неполный заголовок или тело | ждёт данных | открыто |
| EOF посреди кадра | ответа нет | закрывается |
| Длина больше `max_message_length` | `BadRequest` | закрывается |
| Переполнение длины | `BadRequest` | закрывается |
| Неизвестный opcode запроса | `BadRequest` | закрывается |
| Route — не валидный UTF-8 | `BadRequest` | закрывается |
| Некорректные options (пустой key/value, дубликат ключа, лишние байты, неверное число) | `BadRequest` | закрывается |
| Неверная `version` | `BadRequest` (`"Incorrect protocol version"`) | остаётся открытым |

После ошибки декодирования поток нельзя пересинхронизировать, поэтому соединение закрывается. `BadRequest` содержит `req_id` из заголовка, если его удалось прочитать, иначе `0`.

---

## 4. Options

Options — именованные параметры кадра (метаданные, флаги, trace-идентификаторы). Протокол не резервирует ключи; смысл определяет приложение.

| Свойство | Поведение |
|---|---|
| Ключ | произвольные байты, чувствителен к регистру, 1–65535 байт |
| Значение | произвольные байты, 1–65535 байт |
| Пустой ключ или значение | отклоняется |
| Повторяющиеся ключи | отклоняются |
| Количество | 0–255 на кадр |
| Неизвестные ключи | библиотека игнорирует; доступны в `Request.options` |

Wire-формат одной option (`options_count` штук подряд образуют секцию options):

```text
options_key_first = true :  [key_len u16 BE][value_len u16 BE][key][value]
options_key_first = false:  [key_len u16 BE][value_len u16 BE][value][key]
```

Пример — `POW`, payload `7`, option `pow = 3`, `req_id = 2`, раскладка по умолчанию (41 байт):

```text
02 01 03 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 08 00 00 00 01 00 00 00 02 50 4f 57 00 03 00 01 70 6f 77 33 37
```

| Секция | Байты | Значение |
|---|---|---|
| route (3) | `50 4f 57` | `"POW"` |
| options (8) | `00 03` `00 01` `70 6f 77` `33` | `key_len=3`, `value_len=1`, ключ `pow`, значение `3` |
| payload (1) | `37` | `"7"` |

Заголовок: `options_count = 1`, `options_len = 8`, `payload_len = 1`, `req_id = 2`.

Доступ из Rust: `Request::get_option(&self, key: &[u8]) -> Option<Bytes>` (данные не копируются). Значения — сырые байты: разбирайте их сами и **не используйте `unwrap` на значениях от клиента**. Типизированных геттеров нет.

---

## 5. Маршруты и модель соединения

### 5.1. Маршруты

- Route — UTF-8 строка до 255 байт, чувствительна к регистру.
- Неизвестный route → `NotFound` (`"No such method"`); соединение остаётся открытым.
- Повторная регистрация того же route (в том числе между request- и channel-обработчиками) — ошибка.

### 5.2. Состояния соединения

```text
            ChannelConnect                     ChannelConnect
 Fresh ──────────────────────► Channel ◄──────────────────────┐ (Conflict, остаётся)
   │                             │  ▲                          │
   │ Once                        │  └── ChannelMessage ───────┘ (ответа нет)
   ▼                             │
 Once ◄── Once (цикл)            └── Once → mismatch (остаётся)
```

| Состояние | Запрос | Результат | Новое состояние |
|---|---|---|---|
| `Fresh` | `Once` | route-обработчик | `Once` |
| `Fresh` | `ChannelConnect` | `ChannelConnected` | `Channel` |
| `Fresh` | `ChannelMessage` | `BadRequest` («Channel not connected») | `Fresh` |
| `Once` | `Once` | route-обработчик | `Once` |
| `Once` | `ChannelConnect` / `ChannelMessage` | `BadRequest` (несовпадение типа) | `Once` |
| `Channel` | `ChannelConnect` | `Conflict` | `Channel` |
| `Channel` | `ChannelMessage` | channel-обработчик | `Channel` |
| `Channel` | `Once` | `BadRequest` (несовпадение типа) | `Channel` |

Используйте один режим на соединение и не отправляйте `Once` и `ChannelConnect` одновременно на новом соединении.

### 5.3. Запрос/ответ (Once)

- `req_id` выбирает клиент; сервер возвращает его без изменений.
- По одному соединению может идти много запросов (лимиты: 256 на соединение, `max_in_flight` глобально).
- **Ответы могут приходить не в порядке запросов.** Сопоставляйте по `req_id`.
- Если обработчик превысил `request_timeout`, сервер отвечает `Timeout` (11). Отмена срабатывает на ближайшем `.await`; блокирующий код не прерывается.
- Panic обработчика перехватывается и превращается в `InternalError`; соединение остаётся открытым.
- Повтор запроса после таймаута или разрыва — на усмотрение приложения; протокол не гарантирует идемпотентность.

### 5.4. Режим Channel

- `ChannelConnect` переводит соединение в режим Channel; сервер присваивает идентификатор соединения (`conn_id`) и не сообщает его клиенту.
- Сервер отправляет push через `ServerHandle::send_to_channel(conn_id, Response)`. **`conn_id = 0` — рассылка всем channel-соединениям.** Channel-обработчик получает `conn_id` соединения-отправителя.
- `opcode` отправляемого ответа нужно выставить в `ChannelMessage` (8) самостоятельно.
- `Ok(())` от `send_to_channel` означает «принято в очередь рассылки», а не «доставлено». `Err(())` — нет подписчиков.
- Очередь рассылки — 1024 сообщения. Медленный получатель теряет сообщения (`Lagged`).
- Push-сообщения и обычные ответы идут через одну исходящую очередь; их взаимный порядок не гарантируется.

---

## 6. Ошибки

| Условие | Ответ | `req_id` | Соединение | Повтор? |
|---|---|---|---|---|
| Кадр слишком большой / некорректный / неизвестный opcode | `BadRequest` | из заголовка, иначе 0 | закрывается | нет — исправить кадр |
| Неверная версия | `BadRequest` | запроса | открыто | нет |
| Неизвестный route | `NotFound` | запроса | открыто | нет |
| Режим не подходит opcode | `BadRequest` | запроса | открыто | нет |
| Повторный `ChannelConnect` | `Conflict` | запроса | открыто | нет |
| Panic обработчика | `InternalError` | запроса | открыто | зависит от операции |
| Таймаут обработчика | `Timeout` | запроса | открыто | если идемпотентно |
| Ошибка TCP / EOF | нет | — | закрывается | на новом соединении |

`Unauthorized` и `Forbidden` доступны вашим обработчикам; сама библиотека их не отправляет.

---

## 7. Rust API

Публичные модули: `MMA::core::{commands, config, router, server}` и `MMA::c_api`. Framer и сетевой слой приватны. Crate собирается как `rlib`, `cdylib` и `staticlib`.

### 7.1. Типы

| Тип | Роль |
|---|---|
| `Config` | настройки сервера (`Clone`, `Debug`); `Config::default()`, `validate()` |
| `FramerConfig` | wire-раскладка; `FramerConfig::new()` / `Default`, `validate()` |
| `Request` | `headers`, `payload: Bytes`, `route: String`, `options: Vec<(Bytes, Bytes)>` |
| `Response` | `headers`, `payload: Bytes`, `options`; создаётся `Response::new(req_id)` |
| `Router` | таблица маршрутов |
| `Server` | владеет listener; `run`, `run_until` |
| `ServerHandle` | дешёвый `Clone`, `Send + Sync`; отправляет channel-сообщения |

### 7.2. Конфигурация

```rust
use MMA::core::config::{Config, FramerConfig};
use std::{net::SocketAddr, time::Duration};

let config = Config {
    address: SocketAddr::from(([0, 0, 0, 0], 8080)),
    frame_config: FramerConfig::new(),
    max_batch: 255,
    max_in_flight: 1024,
    max_connections: 128_000,
    drain_timeout: Duration::from_secs(2),
    shutdown_grace: Duration::from_secs(10),
    request_timeout: Duration::from_secs(10),
};
config.validate().expect("invalid config");
```

| Параметр | По умолчанию | Смысл |
|---|---|---|
| `max_connections` | 128 000 | одновременные соединения (≥ 1) |
| `max_in_flight` | 1024 | одновременно выполняемые обработчики на весь сервер (≥ 1) |
| `max_batch` | 255 (Rust), 16 (C) | число ответов, записываемых до одного flush (≥ 1) |
| `max_message_length` (в `FramerConfig`) | 10 000 000 | максимальный размер тела в байтах |
| `request_timeout` | 10 с | лимит времени обработчика |
| `drain_timeout` | 2 с | время на отправку накопленных ответов при закрытии соединения |
| `shutdown_grace` | 10 с | ожидание выполняющихся задач при остановке до принудительной отмены |

`Config::validate` отклоняет нулевые лимиты, неверный `FramerConfig` и **порт 0**.

### 7.3. Обработчики

```rust
async fn echo(req: Request, mut res: Response) -> Response {
    res.payload = req.payload;       // Bytes, без копирования
    res
}

let mut router = Router::new();
router.register_route("GET", echo)?;
```

Обработчик принимает `Request` и `Response` по значению и возвращает `Response`. Статус по умолчанию — `Ok`; меняйте `res.headers.opcode` или используйте `Response::bad_request(req_id, Some(payload))`, `Response::conflict(..)` и т.д.

Channel-обработчик:

```rust
use MMA::core::commands::ResponseOpcode;

router.register_channel_handler("SEND".to_string(), {
    let handle = handle.clone();                      // ServerHandle
    move |req: Request, _conn_id: u32| {
        let handle = handle.clone();
        async move {
            let mut msg = Response::new(0);
            msg.headers.opcode = ResponseOpcode::ChannelMessage;
            msg.payload = req.payload;
            let _ = handle.send_to_channel(0, msg);   // 0 = всем
        }
    }
})?;
```

Общее состояние — захватывайте `Arc` в замыкании:

```rust
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

let counter = Arc::new(AtomicU64::new(0));
router.register_route("COUNT", {
    let counter = counter.clone();
    move |_req, mut res: Response| {
        let counter = counter.clone();
        async move {
            res.payload = Bytes::from((counter.fetch_add(1, Ordering::Relaxed) + 1).to_string());
            res
        }
    }
})?;
```

Не блокируйте поток в обработчиках: `request_timeout` не отменяет блокирующий код, а он занимает worker рантайма. Для блокирующей и тяжёлой CPU-работы используйте `tokio::task::spawn_blocking`.

### 7.4. Жизненный цикл сервера

| Метод | Поведение |
|---|---|
| `Server::new(config).await` | проверяет конфигурацию и привязывает listener |
| `Server::from_listener(config, listener)` | использует готовый listener (конфигурация всё равно проверяется, порт 0 отклоняется) |
| `server.handle()` | возвращает `ServerHandle` |
| `server.run(router).await` | работает до Ctrl+C, затем graceful shutdown |
| `server.run_until(router, fut).await` | работает, пока не завершится `fut` |
| `server.shutdown_token()` / `shutdown_server()` | токен отмены / запуск остановки |
| `server.local_addr()` | адрес, на котором слушает сервер |

Остановка: прекращается приём соединений и чтение, выполняющиеся обработчики получают `shutdown_grace`, затем всё отменяется.

### 7.5. Ошибки

| Вызов | Ошибка | Причина |
|---|---|---|
| `Server::new` | `io::Error` | неверная конфигурация или ошибка bind |
| `Router::register_route` / `register_channel_handler` | `Err` | пустой или слишком длинный route, дубликат |
| `ServerHandle::send_to_channel` | `Err(())` | нет подписчиков |
| `Config::validate` | `ConfigError` | нулевой лимит, порт 0, неверный `FramerConfig` |
| `Server::run` / `run_until` | `io::Error` | ошибка listener/accept |

---

## 8. C ABI

Заголовок: [`include/mma.h`](include/mma.h). Библиотека: `libMMA.so` / `libMMA.dylib` / `MMA.dll` и `libMMA.a` в `target/release/` после `cargo build --release`. Проверено только на Linux x86-64.

### 8.1. Функции

| Функция | Назначение | Примечания |
|---|---|---|
| `mma_server_config_default` | заполняет `MMA_ServerConfig` значениями по умолчанию | `bind_ip` указывает на статическую `"0.0.0.0"` |
| `mma_server_create` | проверяет конфигурацию, создаёт сервер (bind ещё нет) | освобождать `mma_server_destroy` |
| `mma_server_register_route` | регистрирует обработчик запрос/ответ | до `start`; route копируется |
| `mma_server_register_channel_handler` | регистрирует channel-обработчик | до `start` |
| `mma_server_route` | алиас `mma_server_register_route` | |
| `mma_server_start` | запускает OS-поток и Tokio runtime, выполняет bind, ждёт готовности | `MMA_OK` только после успешного bind |
| `mma_server_local_port` | возвращает порт | вызывать после `start` |
| `mma_server_get_handle` | получает handle сервера (клон `ServerHandle`) | после `start`; освобождать `mma_server_handle_destroy` |
| `mma_server_handle_send_to_channel` | отправляет response в каналы | `conn_id == 0` — рассылка всем |
| `mma_server_handle_change_config` | отправляет новую конфигурацию работающему серверу | не все поля применяются на лету |
| `mma_server_handle_destroy` | освобождает handle | сервер не останавливает |
| `mma_server_stop` | отменяет работу и ждёт поток сервера | не вызывать из callback этого же сервера |
| `mma_server_destroy` | освобождает сервер (внутри вызывает `stop`) | `NULL` допустим |
| `mma_response_new` / `mma_response_destroy` | создаёт / освобождает самостоятельный response | для сообщений вне callback |
| `mma_response_set_status` | задаёт opcode ответа | возвращает response; `inner == NULL` при ошибке |
| `mma_response_set_payload` | задаёт payload (копируется) | |
| `mma_response_add_option` | добавляет option (копируется) | key/value непустые, ≤ 65535 байт, ≤ 255 options |

### 8.2. Коды ошибок

| Код | Имя | Значение |
|---:|---|---|
| 0 | `MMA_OK` | успех |
| −1 | `MMA_INVALID_ARGUMENT` | `NULL` или неверный аргумент (например, route не UTF-8) |
| −2 | `MMA_INVALID_CONFIG` | конфигурация отклонена или неверный `bind_ip` |
| −3 | `MMA_SERVER_ERROR` | ошибка потока/runtime/bind |
| −4 | `MMA_SERVER_ALREADY_STARTED` | уже запущен |

Ошибки возвращаются только кодом возврата; API last-error нет. Сбой внутри route-callback доходит до клиента как `InternalError` и не виден C-коду.

### 8.3. Callbacks

```c
typedef MMA_Response (*MMA_RouteCallback)(
    MMA_Request request, MMA_Response response, const void *user_data);

typedef int32_t (*MMA_ChannelHandlerCallback)(
    const MMA_Request *request, uint32_t connection_id, const void *user_data);
```

Route-callback — **функциональный**: получает пустой `MMA_Response`, применяет вызовы `mma_response_*` (каждый возвращает обновлённый response) и возвращает его.

Правила:

- Данные `request` (`payload`, `route`, `options`) **заимствованы и действительны только во время callback**. Нужное — копируйте.
- Callbacks выполняются **параллельно на worker-потоках Tokio**. Синхронизируйте общий `user_data` и держите его живым до возврата `mma_server_stop`.
- **Не блокируйте** callback: он занимает worker рантайма, а `request_timeout` его не прервёт.
- Не вызывайте `mma_server_*` из callback; не выбрасывайте исключения C++ и не делайте `longjmp` наружу.
- Чтобы сообщить об ошибке, верните response с `inner == NULL` (или пусть сработает ошибка setter) — клиент получит `InternalError`.
- Возвращаемое значение channel-callback сейчас игнорируется.
- Владение: внутри route-callback response принадлежит библиотеке — верните его, не уничтожайте. Response, созданный `mma_response_new`, требует ровно один `mma_response_destroy`.

### 8.4. Строки и буферы

| Параметр | Длина | NUL | Кодировка | Время жизни |
|---|---|---|---|---|
| `bind_ip` | NUL-terminated | да | текст IPv4/IPv6 | читается только во время вызова |
| `route` | `route_len` | нет | UTF-8, ≤ 255 | копируется |
| `request.payload`, `request.route`, `request.options[i]` | явная длина | нет | байты (route — UTF-8) | только во время callback |
| данные для `mma_response_*` | явная длина | нет | байты | копируются; освободить можно после вызова |

### 8.5. Соответствие типов

| Rust | C |
|---|---|
| `u8` / `u16` / `u32` / `u64` | `uint8_t` / `uint16_t` / `uint32_t` / `uint64_t` |
| `bool` | `uint8_t` (0/1) |
| `Duration` | `uint64_t`, миллисекунды (`*_ms`) |
| `SocketAddr` | `const char *bind_ip` + `uint16_t port` (порт 0 отклоняется) |
| `Bytes` | `const uint8_t *` + `size_t` (заимствование) |
| `String` (route) | `const uint8_t *` + `size_t`, UTF-8, без NUL |
| `Vec<(Bytes, Bytes)>` | `const MMA_Option *` + `size_t` |
| `ResponseOpcode` | `uint8_t`, константы `MMA_RESPONSE_*` |
| `Server` / `ServerHandle` / `Response` | `MMA_Server *` / `MMA_ServerHandle *` / `MMA_Response` |

### 8.6. Правила времени жизни

- `create` → регистрация маршрутов → `start` → … → `stop` → `destroy`.
- Не вызывайте `start`, `stop`, `destroy` конкурентно для одного указателя и не используйте указатель после `destroy`. Библиотека не обнаруживает double-free и use-after-free.
- `mma_server_stop` завершает поток сервера и ждёт его; не гарантирует, что блокирующий callback вернулся.
- `mma_server_handle_change_config`: новые `FramerConfig` и `request_timeout` подхватываются соединениями; `max_batch`, `max_connections`, `max_in_flight`, адрес listener и `shutdown_grace` **не** обновляются. Смена wire-параметров без одновременного переключения клиентов ломает разбор.

### 8.7. Пример

Полный пример — [`examples/main_server.c`](examples/main_server.c) (маршруты `GET`, `POW`, channel-handler `SEND`, обработка сигналов, handle сервера).

```c
#include "mma.h"

static MMA_Response echo(MMA_Request req, MMA_Response res, const void *ud) {
    (void)ud;
    res = mma_response_set_status(res, MMA_RESPONSE_OK);
    if (res.inner == NULL) return res;
    return mma_response_set_payload(res, req.payload, req.payload_len);
}
```

Wire-раскладка задаётся через `config.framer` (`MMA_FramerConfig`) и должна совпадать с клиентской.

---

## 9. Совместимость и версионирование

Три версии независимы:

| Что | Сейчас | Правило |
|---|---|---|
| Wire-протокол | `MMA_VERSION = 1` | повышается при несовместимых изменениях кадра или семантики |
| Rust crate | `0.1.0` | до 1.0 breaking-изменения возможны в любом релизе |
| C ABI / `mma.h` | без версии | сигнатуры могут меняться; запроса версии в рантайме нет |

Два участника совместимы по wire, только если совпадают `MMA_VERSION` **и** весь `FramerConfig`. Новые opcode или обязательное поле требуют новой `MMA_VERSION`; старый сервер закрывает соединение при неизвестном opcode запроса.

---

## 10. Безопасность

- **Нет TLS, аутентификации и защиты от replay.** Конфигурация раскладки — обфускация и не является секретом, если не распространяется как секрет. Завершайте TLS перед MMA (stunnel, sidecar-прокси) или работайте в доверенной сети.
- Считайте все поля от клиента недоверенными. UTF-8 проверяется только у `route`; payload и options — сырые байты.

| Лимит | Значение |
|---|---|
| Тело кадра | `max_message_length` (по умолчанию 10 000 000), проверяется до аллокации |
| Route | ≤ 255 байт |
| Options | ≤ 255 на кадр; key и value по 1–65535 байт |
| Соединения | `max_connections` |
| Запросы в полёте | `max_in_flight` глобально, 256 на соединение |
| Время обработчика | `request_timeout` (только async-код) |

Пока не покрыто: idle/read/write-таймауты (атаки медленными клиентами), глобальный лимит байт в полёте и защита от клиентов, не читающих ответы (см. ниже).

---

## 11. Статус и известные ограничения

MMA — pre-release. Известные проблемы текущей версии:

| Ограничение | Последствие | Обходной путь |
|---|---|---|
| Состояние соединения хранится в общем атомике; состояние `Once` распознаётся ненадёжно, одновременная смена режимов может гоняться | `Once` → `ChannelConnect` может пройти; два режима можно включить одновременно | один режим на соединение |
| Нет idle-, read- и write-таймаутов | медленные или молчащие клиенты удерживают соединения | таймауты на прокси/файрволе |
| Клиент, не читающий ответы, может остановить обработчики и исчерпать `max_in_flight` | отказ в обслуживании | ограничивать клиентов на прокси |
| Ответ, который нельзя закодировать (например, больше `max_message_length`), останавливает запись в соединение | клиент перестаёт получать ответы | держать ответы в пределах лимита |
| Блокирующий код, включая блокирующие C-callbacks, не отменяется `request_timeout` | блокируются worker-потоки | короткие callbacks; блокирующую работу выносить |
| Рассылка всем соединениям с фильтрацией на стороне каждого | O(число соединений) на push | — |
| Нет рукопожатия конфигурации | несовпадающие конфигурации ломаются непредсказуемо | сверять конфигурации вне протокола |
| Закрытие соединения после некорректного кадра предусмотрено, но не покрыто автотестами | — | — |
| `Shutdown` (9) определён, но не отправляется | клиент не получает уведомления об остановке | — |
| Порт 0 отклоняется `Config::validate`, в том числе для C и `from_listener` | нет эфемерного порта | выбрать фиксированный порт |
| Нет Rust-клиента; тесты кодека отключены; в репозитории нет бенчмарков и фаззинга | — | писать клиент по [§3](#3-формат-кадра) |
| C: владение `MMA_Response` при ошибке setter хрупкое; дубликаты регистраций из C могут молча пропасть при старте | — | регистрировать каждый route один раз; проверять коды возврата |
| C: нет запроса версии ABI; константность указателя user-data в `MMA_ChannelHandlerCallback` различается в Rust и заголовке | — | — |

Цифры throughput и latency не публикуются: воспроизводимого бенчмарка в репозитории пока нет. `max_connections = 128 000` — лимит, а не измеренная ёмкость.

---

## 12. Структура репозитория

| Путь | Содержимое |
|---|---|
| `src/protocol/framer.rs` | энкодер/декодер кадров (приватный) |
| `src/network/network.rs` | цикл соединения, машина состояний (приватный) |
| `src/core/` | `commands` (Request/Response), `config`, `router`, `server` |
| `src/c_api.rs` | экспорты C ABI |
| `include/mma.h` | публичный C ABI-заголовок (Go-обёрткой не изменяется) |
| `examples/main_server.rs`, `examples/main_server.c` | примеры Rust- и C-сервера |
| `tests/` | тесты конфигурации (тесты кодека сейчас отключены) |
| `cabi/` | Go-обёртка над существующим C ABI без CGO |
| `native-bridge/bridge.c` | заранее собранный callback-адаптер; не меняет Rust C ABI |
| `.github/workflows/native-release.yml` | сборка и публикация нативных артефактов |

---

## 13. Go-обёртка без CGO

Пакет `github.com/Edestuss0/MMA-Network-protocol/cabi` оборачивает существующие функции из `include/mma.h`. Движок протокола на Rust и публичный C ABI ради Go не переписываются и не меняются.

### Использование в Go

Пользователю необходим Go 1.25+ и одна из платформ: Windows amd64, Linux amd64, macOS amd64 или macOS arm64. Устанавливать Rust, Cargo, CGO, GCC, MinGW или MSVC не нужно. При первом обращении к нативному API обёртка скачивает готовую Rust-библиотеку и соответствующий callback-мост из GitHub Release подключённой версии модуля, проверяет SHA-256 архива и кеширует оба файла.

```powershell
go get github.com/Edestuss0/MMA-Network-protocol/cabi@v0.1.0
```

```go
import "github.com/Edestuss0/MMA-Network-protocol/cabi"
```

Используй версию, для которой уже опубликован релиз с нативными артефактами. Отправка тега (например, `v0.1.0`) запускает `.github/workflows/native-release.yml`: workflow собирает бинарные файлы для поддерживаемых платформ и прикрепляет их к этому релизу. Пока артефакты не опубликованы, обёртка не сможет скачать нативные библиотеки.

### Мост для callback маршрута

В текущем C ABI `MMA_Request` и `MMA_Response` передаются в route-callback по значению. На Windows FFI без CGO не может напрямую принять такой callback с C-структурами по значению. Поэтому `native-bridge/bridge.c` — небольшой адаптер, который заранее собирается в CI вместе с Rust-библиотекой: он принимает исходный callback, вызывает Go-функцию с указателями и возвращает ответ через исходный C ABI. **`include/mma.h`, `src/c_api.rs`, Rust-движок и сигнатуры существующих функций C ABI не изменяются.**

### Локальная проверка и настройки

Для локальной проверки переменная `MMA_NATIVE_DIR` должна указывать на каталог с обоими файлами — Rust-библиотекой и callback-мостом:

| Платформа | Необходимые файлы |
|---|---|
| Windows amd64 | `MMA.dll`, `mma_go_bridge.dll` |
| Linux amd64 | `libMMA.so`, `libmma_go_bridge.so` |
| macOS amd64/arm64 | `libMMA.dylib`, `libmma_go_bridge.dylib` |

- `MMA_NATIVE_VERSION` задаёт тег релиза, например `v0.1.0`.
- `MMA_NATIVE_DIR` использует локальные нативные файлы вместо скачивания.
- `MMA_NATIVE_REFRESH=1` обновляет файлы в кеше при следующей первой загрузке нативного API.

`go test ./cabi` запускает Go-тесты без загрузки нативных библиотек. Интеграционные тесты включаются явно через `go test -tags native_integration ./cabi` и требуют оба нативных файла.
