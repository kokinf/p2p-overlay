# Architecture Decision Record (ADR)

## 1. Параметры
- **Проект:** Децентрализованная оверлейная сеть на Kademlia DHT.
- **Параметры:** `N=12–15`, `K_BUCKET_SIZE=3`, `ALPHA=3`, `R=3`, `MAX_FRAME_PAYLOAD=65536`.

## 2. Подсистемы

- **Transport & Framing (`internal/protocol`)**:
  - TCP-транспорт. Кастомный 24-байтовый заголовок кадра (`version`, `type`, `flags`, `request_id`, `length`), Big-Endian.
  - `StreamDecoder` корректно обрабатывает частичные чтения и склеенные пакеты. `payload_length > MAX_FRAME_PAYLOAD` отклоняется **до** выделения буфера.
  - Кадры с `version != 1` отклоняются в `handleRPC` с `FrameError`; узел остаётся работоспособным .
  - Сериализация: MessagePack (меньше и быстрее JSON). Воспроизводимое сравнение — `TestSerializationComparison → results/serialization.csv`.

- **Identity & Security (`internal/identity`, `internal/rpc`)**:
  - Ed25519. `NodeID = SHA-256(pubkey)`. Ключи сохраняются локально в `NODE_STATE_DIR` (0600).
  - **TLS 1.3 mTLS:** Транспортный уровень использует TLS 1.3 с взаимной аутентификацией. 0-RTT отключён.
  - При рукопожатии узлы обмениваются самоподписанными X.509 сертификатами, сгенерированными из Ed25519 ключей. Из сертификата извлекается публичный ключ, вычисляется `NodeID` и сверяется с ожидаемым (защита от MITM).
  - `InsecureSkipVerify: true` в клиентском конфиге **не** ослабляет проверку: сертификат проверяется вручную (`ExtractNodeIDFromCert` + сверка `NodeID`), что строже стандартного path-валидейшена.
  - **Replay-защита:** на прикладном уровне `replayCache` по `request_id` с TTL 2 минуты. Кэш живёт дольше соединения, поэтому replay после переподключения отклоняется.
  - **Ограничение параллелизма:** семафор на 64 одновременных handler'а на узел (`maxConcurrentHandlers`). Защита от перегрузки одного соединения.
  - **Жизненный цикл:** `cleanupReplayCache` завершается по `done`-каналу в `Manager.Close()`. Утечки горутины нет.

- **DHT & Lookup (`internal/dht`)**:
  - XOR-метрика. 256 k-buckets (по одному на каждый бит), ёмкость каждого — `K_BUCKET_SIZE` (3).
  - Вытеснение по LRU: перед удалением узла отправляется `PING`. Живой LRU сохраняется, мёртвый заменяется. Тесты детерминированы: fake-контакт конструируется в том же bucket через `idSameBucket(self, template)`.
  - Итеративный Lookup: параллельные запросы к `ALPHA=3` ближайшим кандидатам для поиска узлов (`FIND_NODE`) и значений (`FIND_VALUE`).
  - `LookupResult` содержит `InitialClosest` — снимок таблицы **до** поиска, а также полную `Trace` по итерациям. Это позволяет доказать по сырым данным, без ручных манипуляций.
  - Bootstrap через один или несколько известных адресов; после bootstrap — self-lookup. После остановки seed-узла ранее присоединившиеся узлы продолжают выполнять `FIND_NODE` (`TestBootstrapAfterSeedShutdown`).

- **Records & Storage (`internal/records`)**:
  - **Модель:** `NodeRecord` содержит владельца, публичный ключ, адреса, алиас (опционально), версию (`Seq`), время жизни (TTL) и подпись Ed25519.
  - **Каноническое представление:** для подписи данные склеиваются в строго заданном порядке байт (без использования JSON/MsgPack), что исключает атаки на основе неоднозначности сериализации.
  - **Валидация (`Validate`):** связь `NodeID ↔ pubkey`, подпись, `1 ≤ len(Addresses) ≤ 10`, формат `host:port` с портом `1..65535` (`validateAddress`), `len(Alias) ≤ 128`, TTL, `len(CanonicalBytes()) ≤ MaxRecordSize (16 KiB)`.
  - **DHT-ключ:** `AliasKey(alias)` и `NodeRecord.DHTKey()` в одном месте (`records.AliasKey`), префикс `"alias:"`/`"node:"` не дублируется.
  - **Проверка `DHTKey ↔ key`:** на приёме (`handleRPC STORE` — `FrameError` при рассинхроне) и на клиенте (`GetRecord` — отклонение записи, если `DHTKey` не совпал с запрошенным ключом).
  - **Локальное хранилище:** in-memory `Store` с защитой от отката версий (rollback). Записи с меньшим `Seq` отклоняются (`ErrSequenceRollback`). Истекшие по TTL записи не выдаются и удаляются фоновым процессом (10 с).
  - **Репликация:** при сохранении запись отправляется на `R=3` ближайших узла. Операция считается успешной при кворуме (2 из 3, включая локального инициатора). Ошибка локального `Store.Put` (кроме rollback) возвращается вызывающему — репликация без локальной записи бессмысленна.

- **Metrics & Experimentation (`internal/node/metrics.go`)**:
  - `Node.DumpMetrics(path)` сериализует снимок: `NodeID`, адрес, `Buckets`, `Lookups` (с `Trace` и `InitialClosest`), `StoreSize`, `Timestamp`.
  - `experimentLoop` активируется при `Config.Experiment.Enabled = true`: ждёт `Delay`, пишет снимок «до», выполняет `FIND_NODE` по `targets`, пишет снимок «после».
  - Имена файлов: `metrics-<short-id>.json`. Закрытые ключи в файлы не попадают.
