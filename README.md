# Защищённая оверлейная P2P-сеть на Go 1.27.1.

## Сборка и запуск
go mod tidy
go test ./...
go run ./cmd/node

text

## Структура
- cmd/node — точка входа узла
- internal/config — конфигурация YAML
- internal/protocol — кадрирование и потоковый декодер
- internal/transport — TCP-сервер
- internal/node — логика узла
- scripts — утилиты эксперимента