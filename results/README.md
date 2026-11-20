# Результаты

Здесь находятся итоговые таблицы, журналы регрессии и исходные доказательства проверок. Техническая реализация и тесты находятся в internal/, tests/ и scripts/.

| Данные                                   | Что подтверждают                                                                                                                                                                                   |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| serialization.csv                        | Измеренное сравнение размера JSON и MessagePack.                                                                                                                                                   |
| acceptance-suite/                        | Параметры, сводка и результат DHT-матрицы: 30 независимых запусков, 900 контрольных lookup, реальные отказы и восстановление. Полные сценарии, трассы и таблицы — в evidence/acceptance-suite.zip. |
| relay-regression.txt, relay-vet.txt      | Полная Go-регрессия и vet после добавления расширений. Пустой vet-журнал означает отсутствие диагностик.                                                                                           |
| files-validation/                        | Сводка и 15 измерений передачи файлов с SHA-256. Исходный JSONL, конфигурационные профили, версия Go и журнал тестов — в evidence/files-validation.zip.                                            |
| network-lab/                             | Manifest, 60 измерений и итоговая проверка пяти Docker-конфигураций. Конфигурации, метрики, журналы трёх хопов, netem и TCP-доступности — в evidence/network-lab.zip.                              |
| relay-validation/subnet-selection.json   | 200 исходных выборок модели концентрации маршрута. Это модель выбора, не доказательство защиты всей DHT от Sybil.                                                                                  |
| review-20261117/pcap-delivery-check.json | Контроль конфиденциальности с положительной доставкой в окне захвата. Pcap и исходный журнал доставки собраны вместе в evidence/capture-delivery.zip.                                              |
| evidence/manifest.json                   | SHA-256, размер и число файлов каждого архива.                                                                                                                                                     |

Каждый результат относится к снимку исходников и условиям своего эксперимента. Успешный старый прогон не подтверждает автоматически последующую версию. Ограничения методики описаны в docs/VALIDATION.md, FILE_EXPERIMENT.md и NETWORK_EXPERIMENT.md. Сам набор данных не заменяет проверку реализации и не означает автоматическую приёмку преподавателем.

## Просмотр исходных доказательств

Таблицы доступны сразу. Для повторного анализа распакуйте необходимые архивы из корня репозитория:

```powershell
python -m zipfile -e results/evidence/acceptance-suite.zip .
python -m zipfile -e results/evidence/files-validation.zip .
python -m zipfile -e results/evidence/network-lab.zip .
python -m zipfile -e results/evidence/capture-delivery.zip .
```

Архивы восстанавливают пути results/. Файлы внутри сохранены без редактирования; исторические имена тестов могут отличаться от текущих. Ключей и runtime-состояния в архивах нет. Абсолютные пути в сохранённых manifest/Compose отражают машину первоначального запуска: для нового запуска стенда используйте генераторы из scripts/, а не старый Compose без изменений.

После распаковки:

```powershell
python scripts/summarize_acceptance.py --dir results/acceptance-suite --repeats 5
python scripts/summarize_files.py --dir results/files-validation --repeats 5
python scripts/summarize_network_lab.py --dir results/network-lab
New-Item -ItemType Directory -Force run | Out-Null
python scripts/verify_pcap.py --pcap results/review-20261117/control-delivery.pcap --logs results/validation/docker-delivery.txt --output run/pcap-check.json
```

## Уточнённые контракты доверия

trust-validation/ содержит целевые тесты, полную регрессию, vet, три удалённых STORE отдельно от локальной копии и пример из полной матрицы. Все актуальные DHT/file/network архивы обновлены после изменений; manifest фиксирует Go-исходники. Выбор хранителей проверяется среди обнаруженных кандидатов. Только capture-delivery.zip остаётся историческим положительным контролем конфиденциальности. Итог — verification.json; точные гарантии — docs/TRUST_MODEL.md.
