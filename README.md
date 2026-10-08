# p2p-overlay

Защищённая оверлейная P2P-сеть на Go 1.27 
Этап 1 — транспорт и кадрирование.

## Сборка

```
go build -o node.exe ./cmd/node
```

## Тесты

```
go vet ./...
go test ./...
```

## Запуск узла

```
go run ./cmd/node
```

Узел слушает порт из `internal/config/config.yaml` (по умолчанию 9000) и отвечает `PONG` на входящие кадры.

## Структура

```
cmd/node/          — точка входа
internal/config/   — YAML-конфигурация
internal/protocol/ — 24-байтовый кадр, потоковый декодер
internal/transport/— TCP-сервер
internal/node/     — обработка соединений
tests/             — unit и negative-тесты
scripts/           — сравнение сериализации
```

## Формат кадра

| Поле           |  Размер | Порядок    |
| -------------- | ------: | ---------- |
| version        |       1 | —          |
| type           |       1 | —          |
| flags          |       2 | big-endian |
| request_id     |      16 | —          |
| payload_length |       4 | big-endian |
| payload        | ≤ 65536 | —          |

## Сериализация

Основная — MessagePack. Сравнение с JSON:

```
go run ./scripts/compare_serialization.go
```

## Конфигурация

`internal/config/config.yaml`:

```yaml
node:
  port: 9000
  read_timeout: 5s
  write_timeout: 5s
protocol:
  version: 1
  max_frame_payload: 65536
```