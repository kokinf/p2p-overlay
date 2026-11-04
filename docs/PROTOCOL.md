# Спецификация протокола (Фрагмент: Хранение данных)

## 1. Формат записи (NodeRecord)

Запись `NodeRecord` используется для хранения адресной информации узлов и псевдонимов.

| Поле              | Тип        | Описание                               |
| ----------------- | ---------- | -------------------------------------- |
| `node_id`         | `[32]byte` | SHA-256(public_key)                    |
| `public_key`      | `[]byte`   | Публичный ключ Ed25519 (32 байта)      |
| `addresses`       | `[]string` | Массив адресов (например, "ip:port")   |
| `alias`           | `string`   | Псевдоним (опционально, нормализуется) |
| `sequence_number` | `uint64`   | Номер версии (защита от отката)        |
| `issued_at`       | `int64`    | Unix timestamp создания                |
| `expires_at`      | `int64`    | Unix timestamp истечения (TTL)         |
| `signature`       | `[]byte`   | Ed25519 подпись (64 байта)             |

### Каноническое представление для подписи
Чтобы избежать неоднозначности сериализации, подпись вычисляется над строго заданной последовательностью байт:
1. `NodeID` (32 байта)
2. `PublicKey` (32 байта)
3. Длина `Alias` (1 байт) + `Alias` в UTF-8
4. `Seq` (8 байт, Big-Endian)
5. `IssuedAt` (8 байт, Big-Endian)
6. `ExpiresAt` (8 байт, Big-Endian)
7. Для каждого адреса: Длина (1 байт) + Адрес в UTF-8

### Вычисление ключа DHT
- Для узла: `Key = SHA-256("node:" + node_id_hex)`
- Для псевдонима: `Key = SHA-256("alias:" + normalize(alias))`
Нормализация: перевод в нижний регистр, удаление пробелов по краям.

## 2. Сообщения RPC

### `STORE_REQUEST` (Тип 5)
Запрос на сохранение записи.
*Payload (MessagePack):*
- `key`: `[32]byte`
- `record`: сериализованный `NodeRecord` (MessagePack)

### `STORE_RESPONSE` (Тип 6)
Ответ на сохранение.
*Payload (MessagePack):*
- `success`: `bool` (true, если сохранено или если `sequence_number` ниже текущего)

### `FIND_VALUE_REQUEST` (Тип 7)
Запрос на поиск значения по ключу.
*Payload (MessagePack):*
- `key`: `[32]byte`

### `FIND_VALUE_RESPONSE` (Тип 8)
Ответ на поиск.
*Payload (MessagePack):*
- `record`: сериализованный `NodeRecord` (если найден)
- `contacts`: массив ближайших контактов (если запись не найдена)