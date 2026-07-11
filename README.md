# Boarkshop

Boarkshop — self-hosted демон на Go, который принимает HTTP-, Telegram- и Cron-события и передаёт каждое событие всем активным пользовательским пайплайнам. Пайплайн сначала запускает `guard`, а после принятия события последовательно запускает остальные команды.

Текущий MVP:

- собирается в один бинарный файл;
- хранит конфигурацию, ресурсы и данные локально;
- принимает произвольные HTTP-методы и пути;
- получает обновления нескольких Telegram-ботов через long polling;
- создаёт события по пяти-польным Cron-расписаниям;
- выполняет команды как массив `argv`, без неявного shell;
- ограничивает очередь событий и число одновременно работающих процессов.

Команды пайплайна считаются доверенным кодом и запускаются с правами пользователя Boarkshop. Встроенной песочницы и шифрования секретов в MVP нет.

## Быстрый старт

Для сборки нужен Go 1.24 или новее. Готовый пример пайплайна использует POSIX shell `sh`, `grep`, `cp` и `mv`; на Windows его удобно запускать в WSL/Git Bash или заменить команды в manifest на Windows-эквиваленты.

Из корня репозитория:

```sh
mkdir -p bin
go build -o ./bin/boarkshop ./cmd/boarkshop
```

Проверьте instance config и все найденные manifests:

```sh
./bin/boarkshop validate --config ./examples/boarkshop.yaml
```

Ожидаемый результат:

```text
configuration is valid
```

Запустите демон в первом терминале:

```sh
./bin/boarkshop serve --config ./examples/boarkshop.yaml --log-level debug
```

Команду `serve` можно опустить: запуск `./bin/boarkshop --config ./examples/boarkshop.yaml` эквивалентен ей.

Во втором терминале отправьте событие, которое принимает пример `http-demo`:

```sh
curl -i \
  -X POST \
  -H 'Content-Type: application/json' \
  --data '{"message":"hello from curl"}' \
  http://127.0.0.1:8080/demo
```

HTTP listener вернёт `202 Accepted` после помещения события в память. Это не подтверждение успешного выполнения пайплайна. После обработки копия события появится здесь:

```sh
cat ./examples/.runtime/pipelines/http-demo/last-event.json
```

Запросы с другим методом или путём также принимаются HTTP listener-ом, но guard примера завершится с кодом `1`, и шаг `persist-event` не запустится.

Остановите демон через `Ctrl+C`.

## CLI

В MVP доступны:

```text
boarkshop [serve] [--config PATH] [--log-level debug|info|warn|error]
boarkshop validate [--config PATH]
boarkshop version
```

Если `--config` не указан, используется переменная `BOARKSHOP_CONFIG`, а затем файл `boarkshop.yaml` в текущей директории.

`validate` строго разбирает YAML, проверяет manifests и ссылки на ресурсы, а также разрешает секреты активных pipelines и Telegram-ботов. При этом команда не привязывает сетевые порты, не обращается к Telegram и не запускает команды пайплайнов.

## Файловая модель

Пути `data_dir` и `pipelines_dir` задаются в instance config. Для примера дерево после первого запуска выглядит так:

```text
examples/
├── boarkshop.yaml
├── pipelines/
│   └── http-demo/
│       ├── pipeline.yaml
│       ├── guard.sh
│       └── process.sh
└── .runtime/
    ├── runs/                 # временные директории запусков
    ├── pipelines/http-demo/  # постоянные данные пайплайна
    └── shared/               # общие постоянные данные
```

Временная директория отдельного запуска удаляется после завершения. Определение пайплайна в `pipelines_dir` и его изменяемые данные в `data_dir/pipelines` не смешиваются.

## Документация

Точная схема YAML, контракты событий, переменные окружения, placeholders и ограничения описаны в [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

Проверка проекта:

```sh
go test ./...
```
