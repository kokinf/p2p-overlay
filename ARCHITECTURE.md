# Architecture Decision Record

## Уровень сложности
Упрощённый. Обоснование: соответствует рекомендуемому N=12–15, k=3,
достаточен для демонстрации невырожденной DHT и туннелей с 2 ретрансляторами.

## Язык и библиотеки
- Go 1.27.1 (generic methods, encoding/json/v2, net, crypto/tls).
- github.com/vmihailenco/msgpack/v5 — сериализация.
- gopkg.in/yaml.v3 — конфигурация.
