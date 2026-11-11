# Спецификация протокола и Модель угроз

## 1. Модель угроз (Threat Model)

При проектировании сети учитывались следующие атаки:

### Защищенные векторы атак:
1. **Пассивный перехват (Sniffing):** TLS 1.3 (шифрование AEAD) и E2E-шифрование (AES-GCM) внутри туннеля.
2. **MITM (Man-in-the-Middle):** mTLS с проверкой соответствия сертификата и `NodeID` (Ed25519).
3. **Подмена DHT-записи:** Все записи подписываются Ed25519 ключом владельца.
4. **Replay-атака:** Защита на уровне TLS + прикладной `replayCache` по `request_id` (TTL 2 минуты). На уровне туннеля — дедупликация по `message_id`.
5. **Откат версии (Rollback):** Монотонное увеличение `sequence_number`; `Store.Put` возвращает `ErrSequenceRollback`.
6. **Несоответствие DHT-ключа:** Проверка `rec.DHTKey() == key` на приёме и на клиенте.
7. **Неизвестная версия кадра:** `handleRPC` возвращает `FrameError`.
8. **Перехват на ретрансляторе:** Ретранслятор видит только `TunnelData` (ciphertext), но не открытый текст.

### Ограничения модели (Out of Scope):
*   **Глобальный наблюдатель:** Система не скрывает факт соединения узлов и объем трафика.
*   **Sybil-атаки:** Модель не защищает от создания большого количества фальшивых узлов.
*   **Компрометация локального узла:** Если злоумышленник получил доступ к `identity.key`.

## 2. Формат кадра

Каждый кадр имеет фиксированный 24-байтовый заголовок (Big-Endian):
| Поле             | Размер | Описание                                                  |
| ---------------- | ------ | --------------------------------------------------------- |
| `version`        | 1      | Версия протокола (сейчас `1`)                             |
| `type`           | 1      | Тип сообщения                                             |
| `flags`          | 2      | Служебные флаги                                           |
| `request_id`     | 16     | Уникальный ID запроса (для корреляции и защиты от Replay) |
| `payload_length` | 4      | Длина payload                                             |
| `payload`        | N      | Данные (MessagePack)                                      |

- **Длина:** `payload_length > MAX_FRAME_PAYLOAD (65536)` отклоняется **до** выделения буфера.
- **Корреляция:** ответ должен содержать тот же `request_id`.
- **Replay:** на приёме `request_id` попадает в `replayCache` (TTL 2 минуты).

## 3. Сетевые уровни (Underlay vs Overlay)

При развертывании через Docker четко разделяются четыре графа:
1. **Underlay:** Docker bridge-сеть (`p2p-net`). Обеспечивает IP-достижимость между контейнерами.
2. **Bootstrap-граф:** Схемы `star`, `ring`, `tree`, `multi-seed` (задаются в `docker-compose.yml`).
3. **DHT-overlay:** Контакты, реально находящиеся в k-bucket после discovery и refresh.
4. **Туннельный маршрут:** Последовательность ретрансляторов (Sender → R1 → R2 → R3 → Receiver).

Путь DHT-поиска (FIND_NODE) не считается туннелем. Прямая отправка без ретрансляторов не засчитывается как туннельная доставка.

## 4. Формат записи (NodeRecord)

| Поле              | Тип        | Описание                          |
| ----------------- | ---------- | --------------------------------- |
| `node_id`         | `[32]byte` | SHA-256(public_key)               |
| `public_key`      | `[]byte`   | Публичный ключ Ed25519 (32 байта) |
| `addresses`       | `[]string` | Массив адресов ("ip:port")        |
| `alias`           | `string`   | Псевдоним (опционально)           |
| `sequence_number` | `uint64`   | Номер версии (защита от отката)   |
| `issued_at`       | `int64`    | Unix timestamp создания           |
| `expires_at`      | `int64`    | Unix timestamp истечения (TTL)    |
| `signature`       | `[]byte`   | Ed25519 подпись (64 байта)        |

### Каноническое представление для подписи
1. `NodeID` (32 байта)
2. `PublicKey` (32 байта)
3. Длина `Alias` (1 байт) + `Alias` в UTF-8
4. `Seq` (8 байт, Big-Endian)
5. `IssuedAt` (8 байт, Big-Endian)
6. `ExpiresAt` (8 байт, Big-Endian)
7. Для каждого адреса: Длина (1 байт) + Адрес в UTF-8

## 5. Сообщения RPC (Payload)

| `type` | Имя                   | Payload                                                           |
| -----: | --------------------- | ----------------------------------------------------------------- |
|      1 | `PING`                | `{addr?: string}`                                                 |
|      2 | `PONG`                | —                                                                 |
|      3 | `FIND_NODE_REQUEST`   | `{target: [32]byte, addr?: string}`                               |
|      4 | `FIND_NODE_RESPONSE`  | `{contacts: [{node_id, address}]}`                                |
|      5 | `STORE_REQUEST`       | `{key: [32]byte, record: []byte}`                                 |
|      6 | `STORE_RESPONSE`      | `{success: bool}`                                                 |
|      7 | `FIND_VALUE_REQUEST`  | `{key: [32]byte, addr?: string}`                                  |
|      8 | `FIND_VALUE_RESPONSE` | `{record?: []byte, contacts?: [{node_id, address}]}`              |
|      9 | `TUNNEL_BUILD`        | `{tunnel_id, path, hop_index, sender_id, e2e_pub, e2e_sig, ...}`  |
|     10 | `TUNNEL_BUILD_OK`     | `{tunnel_id, hop_index, accepted, receiver_id, receiver_e2e_pub}` |
|     12 | `TUNNEL_DATA`         | `{tunnel_id, hop_index, seq, nonce, ct}`                          |
|     13 | `TUNNEL_ACK`          | `{tunnel_id, seq, success}`                                       |
|     14 | `TUNNEL_CLOSE`        | `{tunnel_id, reason}`                                             |
|     15 | `TUNNEL_CLOSE_OK`     | —                                                                 |
|     18 | `ERROR`               | `[]byte` — текст ошибки                                           |

### Обновление RT в запросах
Для предотвращения ситуаций, когда узел узнает эфемерный порт вместо реального адреса прослушивания, поля `addr` в `PING`, `FIND_NODE_REQUEST` и `FIND_VALUE_REQUEST` передают фактический адрес инициатора. Принимающая сторона обновляет таблицу маршрутизации на основе этого поля.

## 6. Туннели (Типы 9–17)

### `TUNNEL_BUILD`
Инициатор отправляет первому ретранслятору полный `Path`. Каждый ретранслятор инкрементирует `HopIndex`, сохраняет `RelayState` и пересылает запрос дальше. Получатель генерирует эфемерный X25519 ключ и возвращает его в `receiver_e2e_pub`.

### `TUNNEL_DATA`
Содержит `Ciphertext` (AES-256-GCM). Ретрансляторы не могут его расшифровать, они просто пересылают пакет следующему узлу.

### `APP_MESSAGE` (Тип 16)
Передаётся **внутри** зашифрованного `TUNNEL_DATA`. Содержит `message_id` (16 байт) и `payload` (открытый текст). Получатель ведет словарь `SeenMsgs` для защиты от повторной обработки дубликатов.

## 7. Lookup и его след

`FIND_NODE` / `FIND_VALUE` выполняются итеративно. Каждый lookup оставляет `LookupResult`:

```json
{
  "target": "<hex>",
  "initial_closest": ["<hex>", "..."],
  "closest": ["<hex>", "..."],
  "rpcs": 4,
  "iterations": 2,
  "timeouts": 0,
  "errors": 0,
  "trace": [
    {"index": 0, "queried": ["<hex>"], "replies": 1, "errors": 0, "timeouts": 0}
  ],
  "started": "...", "finished": "...", "duration_ms": 43
}
```

- `initial_closest` — снимок таблицы инициатора **до** поиска;
- `closest` — итоговый shortlist;
- `trace` — по итерациям.
