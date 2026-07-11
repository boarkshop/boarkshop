# Конфигурация Boarkshop MVP

Boarkshop читает один instance config и по одному `pipeline.yaml` из директорий локальных пайплайнов. Все YAML-файлы имеют `version: 1`, разбираются в строгом режиме и могут содержать только одно YAML-представление: неизвестные поля и второй документ после `---` считаются ошибками.

Значения длительности записываются строками в формате Go `time.ParseDuration`, например `250ms`, `30s`, `5m` или `1h30m`. Там, где длительность обязательна, она должна быть положительной.

## Instance config

Путь передаётся через `--config`. Если флаг отсутствует, CLI использует `BOARKSHOP_CONFIG`, а затем `boarkshop.yaml` в текущей директории.

Относительные `data_dir`, `pipelines_dir` и ссылки на файлы Telegram-токенов разрешаются относительно директории instance config, а не относительно текущей рабочей директории процесса.

Полная текущая схема:

```yaml
version: 1

data_dir: data
pipelines_dir: pipelines
queue_size: 1024
max_parallel_processes: 4
shutdown_timeout: 30s

listeners:
  http:
    enabled: false
    address: 127.0.0.1:8080
    max_body_bytes: 1048576
    read_header_timeout: 5s

  telegram:
    bots:
      - id: main-bot
        token:
          env: TELEGRAM_BOT_TOKEN
          # file: secrets/telegram-token
        api_base: https://api.telegram.org
        poll_timeout: 30s

  cron:
    timezone: UTC
    schedules:
      - id: hourly
        expression: "0 * * * *"
```

В `token` нужно оставить ровно один источник: `env` или `file`.

### Корневые поля

| Поле | По умолчанию | Требования |
|---|---:|---|
| `version` | нет | Обязательно, только `1`. |
| `data_dir` | `data` рядом с config | Непустой путь. Здесь создаются `runs`, `pipelines` и `shared`. |
| `pipelines_dir` | `pipelines` рядом с config | Непустой путь; `pipelines_dir` и `data_dir` не могут совпадать или быть вложены друг в друга. |
| `queue_size` | `1024` | Положительное целое; размер bounded in-memory очереди. |
| `max_parallel_processes` | `4` | Положительное целое; также определяет число event workers. |
| `shutdown_timeout` | `30s` | Положительная длительность дренирования очереди и остановки. |
| `listeners` | пустые listeners с defaults | Настройки HTTP, Telegram и Cron. |

### `listeners.http`

| Поле | По умолчанию | Требования |
|---|---:|---|
| `enabled` | `false` | Если `true`, демон привязывает HTTP address. |
| `address` | `127.0.0.1:8080` | Значение `host:port`; порт от 1 до 65535. Для IPv6 используйте, например, `[::1]:8080`. |
| `max_body_bytes` | `1048576` | Положительное число байтов. Превышение даёт HTTP 413. |
| `read_header_timeout` | `5s` | Положительная длительность чтения заголовков. |

HTTP listener принимает все методы и пути. Встроенного health endpoint нет: любой запрос, дошедший до listener-а и прошедший транспортные лимиты, становится событием.

### `listeners.telegram.bots[]`

| Поле | По умолчанию | Требования |
|---|---:|---|
| `id` | нет | Обязательно, уникально; `[A-Za-z0-9][A-Za-z0-9._-]*`, не более 128 символов. Это локальное имя, не Telegram username. |
| `token.env` | нет | Имя существующей переменной окружения. Взаимоисключается с `token.file`. |
| `token.file` | нет | Путь к читаемому файлу. Взаимоисключается с `token.env`. |
| `api_base` | `https://api.telegram.org` | Абсолютный HTTP(S) URL. Обычным deployments следует использовать HTTPS; HTTP полезен прежде всего для локальных тестов. |
| `poll_timeout` | `30s` | Положительная длительность. В запросе Bot API она округляется вверх до секунд и ограничивается диапазоном 1–50 секунд. |

Каждый бот опрашивается независимо через Telegram Bot API `getUpdates`. Один instance поддерживает несколько ботов; их `id` и разрешённые значения токенов должны быть уникальны. Дубликат отклоняется при `validate` и старте. Токен не включается в событие.

Файл токена читается при `validate` и старте. Из его конца удаляется одна последовательность перевода строки `LF` или `CRLF`; остальные пробелы являются частью токена. Значение не может содержать NUL.

### `listeners.cron`

| Поле | По умолчанию | Требования |
|---|---:|---|
| `timezone` | `UTC` | Имя, принимаемое Go `time.LoadLocation`, например `UTC` или `Europe/Tbilisi`. |
| `schedules` | `[]` | Список расписаний; пустой список не запускает Cron listener. |
| `schedules[].id` | нет | Обязательно, уникально; `[A-Za-z0-9][A-Za-z0-9._-]*`, не более 128 символов. |
| `schedules[].expression` | нет | Стандартное пяти-польное Cron-выражение. |

Порядок пяти полей:

```text
минута час день-месяца месяц день-недели
```

Например, `*/5 * * * *` запускается каждые пять минут, а `0 9 * * 1-5` — в 09:00 по будням в заданной `timezone`. Поле секунд не поддерживается. Одно значение `timezone` применяется ко всем schedules instance-а.

Cron вычисляет следующую точку от текущего времени. Пропущенные срабатывания после остановки или перегрузки не воспроизводятся и catch-up очереди нет.

## Pipeline manifest

Boarkshop просматривает только непосредственные дочерние директории `pipelines_dir` и ищет в каждой файл с точным именем `pipeline.yaml`. Более глубокие manifests игнорируются. Все найденные manifests загружаются и проверяются; выполняются только pipelines с `enabled: true`.

Текущая схема:

```yaml
version: 1
id: example
enabled: true

env:
  MODE: production

secrets:
  API_TOKEN:
    env: SOURCE_API_TOKEN
  SIGNING_KEY:
    file: secrets/signing-key

resources:
  - scripts/guard.sh
  - scripts/process.sh

guard:
  argv:
    - sh
    - "{{pipeline_dir}}/scripts/guard.sh"
  timeout: 5s

steps:
  - id: process
    argv:
      - sh
      - "{{pipeline_dir}}/scripts/process.sh"
    timeout: 30s
```

### Поля manifest

| Поле | По умолчанию | Требования |
|---|---:|---|
| `version` | нет | Обязательно, только `1`. |
| `id` | нет | Обязательно, уникально среди pipelines без учёта регистра; `[A-Za-z0-9][A-Za-z0-9._-]*`, не более 128 символов. |
| `enabled` | `true` | `false` отключает выполнение, но manifest всё равно должен быть корректным. |
| `env` | `{}` | Статические переменные окружения команд. Имена: `[A-Za-z_][A-Za-z0-9_]*`. |
| `secrets` | `{}` | Значения, загружаемые в переменные окружения из env или файла. |
| `resources` | `[]` | Существующие относительные пути внутри директории pipeline. |
| `guard.argv` | нет | Обязательный непустой массив аргументов. Первый элемент — executable. |
| `guard.timeout` | `30s` | Положительная длительность. |
| `steps` | `[]` | Последовательно выполняемые шаги после принятия guard. |
| `steps[].id` | нет | Обязательно и уникально внутри pipeline; формат ID как выше. |
| `steps[].argv` | нет | Обязательный непустой массив аргументов. |
| `steps[].timeout` | `30s` | Положительная длительность. |

Имена назначения в `env` и `secrets` не могут начинаться с `BOARKSHOP_` без учёта регистра. Одно имя нельзя одновременно объявить в `env` и `secrets`.

Каждый `secrets.<NAME>` задаёт ровно один источник:

```yaml
secrets:
  TOKEN_FROM_ENV:
    env: HOST_TOKEN
  TOKEN_FROM_FILE:
    file: secrets/token
```

Источник `env` читается из окружения самого демона. `file` должен быть относительным путём к существующему обычному файлу внутри pipeline; абсолютные пути, выход из pipeline через `..` и символические ссылки в компонентах относительной ссылки запрещены. Финальный перевод строки удаляется так же, как у Telegram token.

Элементы `resources` также должны существовать внутри pipeline и не могут проходить через символические ссылки. Этот список проверяет наличие ожидаемых ресурсов; доступ к ним команда получает через `{{pipeline_dir}}` или `BOARKSHOP_PIPELINE_DIR`.

В текущем manifest нет полей `retry`, `dedup`, `on_error` или отдельных типов шагов. Из-за строгого YAML такие поля отклоняются. Повторных попыток нет, а любая ошибка шага останавливает текущий запуск pipeline.

## Запуск команд

`argv` передаётся процессу напрямую. Boarkshop не запускает shell, не разбивает строки по пробелам, не выполняет shell quoting и не подставляет `$VARIABLE` в аргументы.

Правильно — один элемент YAML на один аргумент:

```yaml
argv: [python3, "{{pipeline_dir}}/scripts/process.py", --mode, safe]
```

Для shell-конструкций shell нужно запросить явно:

```yaml
argv: [/bin/sh, -c, 'printf "%s\n" "$BOARKSHOP_EVENT_ID"']
```

Рабочая директория каждой команды — временная директория текущего запуска, а не директория pipeline. Поэтому относительный `./scripts/process.sh` обычно не найдёт ресурс; используйте placeholder или переменную окружения pipeline directory.

### Placeholders в `argv`

Во всех элементах `guard.argv` и `steps[].argv` выполняется буквальная замена:

| Placeholder | Значение |
|---|---|
| `{{pipeline_dir}}` | Абсолютный путь к директории с `pipeline.yaml`. |
| `{{run_dir}}` | Временная директория текущего запуска. |
| `{{data_dir}}` | Постоянная директория данных текущего pipeline. |
| `{{shared_dir}}` | Общая постоянная директория instance-а. |
| `{{event_file}}` | Абсолютный путь к `event.json` в run directory. |

Замена выполняется внутри каждого готового аргумента и не добавляет quoting. Остальной шаблонизации или интерполяции в MVP нет.

### Переменные окружения команд

Команда наследует окружение демона, затем получает `env`, разрешённые `secrets` и зарезервированные runtime-переменные:

| Переменная | Значение |
|---|---|
| `BOARKSHOP_PIPELINE_DIR` | Абсолютная директория определения pipeline. |
| `BOARKSHOP_RUN_DIR` | Временная директория запуска. |
| `BOARKSHOP_DATA_DIR` | Постоянная директория этого pipeline. |
| `BOARKSHOP_SHARED_DIR` | Общая постоянная директория instance-а. |
| `BOARKSHOP_EVENT_FILE` | Путь к полному JSON события. |
| `BOARKSHOP_EVENT_ID` | 32-символьный ID события. |
| `BOARKSHOP_RUN_ID` | 32-символьный ID запуска pipeline. |
| `BOARKSHOP_PIPELINE_ID` | ID pipeline. |
| `BOARKSHOP_STEP_ID` | `guard` или ID текущего шага. |
| `BOARKSHOP_SOURCE` | `http`, `telegram` или `cron`. |

Run directory имеет права `0700`, а `event.json` — `0600` на POSIX-системах. Одна run directory используется guard и всеми шагами, после чего удаляется. `BOARKSHOP_DATA_DIR` и `BOARKSHOP_SHARED_DIR` сохраняются между запусками.

Stdout и stderr каждого процесса ограничены первыми 64 KiB на поток. Они попадают в структурированный лог только при `--log-level debug`. Не выводите секреты в stdout/stderr.

На timeout или остановке демон завершает процесс; на Unix он также пытается завершить выделенную ему process group. Pipeline-команды одного pipeline выполняются по одному запуску за раз, а разные pipelines могут работать параллельно в пределах `max_parallel_processes`.

## Guard и шаги

Guard всегда запускается первым и интерпретируется по exit code:

| Exit code | Результат |
|---:|---|
| `0` | Событие принято; запускаются шаги. |
| `1` | Событие штатно отклонено; шаги не запускаются. |
| другой | Ошибка guard; шаги не запускаются. |

Timeout или отмена guard также являются ошибкой/отменой запуска. Для обычного шага только exit code `0` означает успех; ненулевой код, timeout или отмена останавливают последующие шаги этого pipeline. Результат одного pipeline не влияет на остальные pipelines, получившие то же событие.

## Общий контракт события

Listener записывает полный транспортно-специфичный JSON прямо в корень `event.json`, без оболочки `payload`. Во всех событиях schema version 1 присутствуют:

| Поле | Тип | Контракт |
|---|---|---|
| `source` | string | `http`, `telegram` или `cron`. |
| `schema_version` | integer | Сейчас `1`. |
| `event_id` | string | Случайный 128-bit ID как 32 lowercase hex-символа. |
| `received_at` | string | UTC timestamp в RFC3339Nano. |

Файл кодируется компактным JSON. Форматирование ниже добавлено только для читаемости.

## HTTP event schema

```json
{
  "source": "http",
  "schema_version": 1,
  "event_id": "0123456789abcdef0123456789abcdef",
  "received_at": "2026-07-11T12:00:00.123456789Z",
  "method": "POST",
  "path": "/demo",
  "request": {
    "query": {
      "tag": ["one", "two"]
    },
    "headers": {
      "Content-Type": ["application/json"]
    },
    "body_base64": "eyJtZXNzYWdlIjoiaGVsbG8ifQ==",
    "body_text": "{\"message\":\"hello\"}",
    "body_json": {
      "message": "hello"
    },
    "remote_address": "127.0.0.1"
  }
}
```

Контракт `request`:

- `query` всегда присутствует как object, каждое значение — массив строк;
- `headers` всегда присутствует как object, каждое значение — массив строк;
- `body_base64` всегда присутствует и без потерь хранит исходные байты;
- `body_text` присутствует только для корректного UTF-8 тела;
- `body_json` присутствует только для непустого корректного JSON и может быть любым JSON value;
- `remote_address` содержит адрес клиента без TCP-порта, если его удалось отделить.

Listener отвечает `202 Accepted`, когда событие помещено в очередь. При заполненной/закрытой очереди он отвечает `503 Service Unavailable`, при превышении `max_body_bytes` — `413 Request Entity Too Large`, а при другой ошибке чтения тела — `400 Bad Request`. Результаты guard и шагов не меняют HTTP response.

HTTP listener не проверяет аутентификацию, подписи GitHub/Stripe и бизнес-смысл body. Такие проверки принадлежат guard или последующим командам.

## Telegram event schema

```json
{
  "source": "telegram",
  "schema_version": 1,
  "event_id": "0123456789abcdef0123456789abcdef",
  "received_at": "2026-07-11T12:00:00.123456789Z",
  "bot_id": "main-bot",
  "update_id": 123456,
  "update_type": "message",
  "chat_id": 10001,
  "user_id": 20002,
  "telegram": {
    "update": {
      "update_id": 123456,
      "message": {}
    }
  }
}
```

`bot_id`, `update_id` и `update_type` всегда присутствуют. `chat_id` и `user_id` тоже всегда присутствуют, но имеют значение `null`, если listener не смог извлечь соответствующий ID из данного типа update. `telegram.update` содержит полный исходный update как JSON object. Bot token в событие не попадает.

При временной ошибке Telegram API или backpressure listener не продвигает текущий offset и повторяет polling после задержки. Offset процесса и сама очередь не являются durable state; успешное помещение в очередь ещё не означает завершение pipelines.

## Cron event schema

```json
{
  "source": "cron",
  "schema_version": 1,
  "event_id": "0123456789abcdef0123456789abcdef",
  "received_at": "2026-07-11T12:00:00.123456789Z",
  "schedule_id": "hourly",
  "expression": "0 * * * *",
  "timezone": "UTC",
  "scheduled_at": "2026-07-11T12:00:00Z",
  "triggered_at": "2026-07-11T12:00:00.123456789Z",
  "cron": {
    "schedule_id": "hourly",
    "expression": "0 * * * *"
  }
}
```

`scheduled_at` и `triggered_at` всегда записываются в UTC/RFC3339Nano, даже если расписание вычисляется в другой timezone. `received_at` совпадает с моментом `triggered_at`.

Если in-memory очередь не принимает Cron event, это срабатывание отбрасывается. Cron не повторяет его и не воспроизводит пропущенные точки позже.

## Delivery и эксплуатационные ограничения

- Очередь bounded и хранится только в памяти. После аварийного завершения уже принятые события могут быть потеряны.
- HTTP `202` подтверждает admission в очередь, а не успешную обработку.
- Durable spool, retries шагов и дедупликация в текущем MVP отсутствуют.
- Каждое событие fan-out-ится всем активным pipelines; несколько pipelines могут его принять.
- Запуски одного pipeline сериализованы, но строгий FIFO-порядок событий не является контрактом.
- Cron drops событие при backpressure; HTTP возвращает 503; Telegram повторяет текущий update.
- Изменения instance config, manifests и списка Telegram-ботов требуют перезапуска демона.

## Безопасность

- Pipeline — доверенный произвольный код. Путевые переменные дают удобный доступ к директориям, но не ограничивают доступ процесса к файловой системе, сети или другим ресурсам пользователя демона.
- Запускайте Boarkshop под отдельным непривилегированным OS-пользователем и ограничивайте права на config, pipelines и data directories.
- Секреты находятся в plaintext-файлах или окружении и передаются дочернему процессу через env. Встроенного шифрования нет.
- Команды наследуют окружение демона. Не помещайте туда значения, которые нельзя доверить всем активным pipelines.
- `BOARKSHOP_SHARED_DIR` доступна всем pipelines instance-а и не обеспечивает изоляцию.
- HTTP listener не предоставляет TLS, auth, rate limiting или проверку webhook signatures. По умолчанию он слушает только loopback; для внешнего доступа используйте защищённый reverse proxy и guard-проверки.
- HTTP headers и body могут содержать секреты и записываются во временный `event.json`. Не логируйте их из pipeline-команд.
- В debug-режиме stdout/stderr команд попадают в лог. Команда сама отвечает за отсутствие секретов в выводе.
