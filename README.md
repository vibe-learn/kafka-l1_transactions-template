        # kafka — Транзакции и end-to-end exactly-once

        Homework-шаблон для урока **l1_transactions** (Транзакции и end-to-end exactly-once) на платформе Vibe Learn.

        ## Что делать

        Реализуй transactional producer на Go (confluent-kafka-go). Сервис читает топик
`usage_events`, агрегирует по `customer_id` за batch и пишет в `hourly_charges`
с EOS-гарантиями через `sendOffsetsToTransaction()`.

Chaos-тест (входит в шаблон):
- Запускает processor, посылает 50 usage_events в 5 batches по 10.
- После каждого второго batch убивает процессор mid-transaction (SIGKILL).
- Перезапускает processor.
- Читает `hourly_charges` с `isolation.level=read_committed` и проверяет:
  - Каждый customer_id встречается ровно столько раз, сколько batches он вошёл.
  - Нет дублей (duplicate charge_id).
  - Нет потерь (sum of charges == sum of input usage).

CI-assert: `go test ./... -run TestTransactionalEOS` должен проходить за < 30 сек.

## Контекст (из transfer-задачи урока)

**Сценарий: Billing Service и exactly-once.**

Твой сервис `billing-processor` обрабатывает Kafka-топик `usage_events`:
consumer читает событие об использовании ресурса (например, 1 час VM), агрегирует
по `customer_id` за час и пишет итог в топик `hourly_charges`. SLA требует
**exactly-once**: каждый usage_event должен попасть ровно в одну charge-запись,
без дублей и без потерь.

## Recap из урока

- **idempotence** защищает от дубля при ретрае в одной партиции; **транзакции** гарантируют атомарную запись в несколько партиций и топиков одновременно.
- **transactional.id** — стабильный идентификатор продьюсера; при рестарте увеличивает эпоху и автоматически fencing'ует предыдущий экземпляр.
- **sendOffsetsToTransaction()** атомарно включает consumer-оффсеты в транзакцию: если транзакция абортируется — оффсеты откатываются, процессор перечитывает ровно тот batch.
- **LSO** (Last Stable Offset) — граница для `read_committed` consumer: он не продвигается дальше, пока в партиции есть незакоммиченные транзакции.
- Транзакции стоят **5–30% throughput** из-за дополнительных round-trip к Transaction Coordinator. Используй EOS там, где цена дубля или потери выше этой стоимости.

        ## Как работать

        1. Платформа Vibe Learn создаёт копию этого репо в твоём GitHub-аккаунте по клику «Начать домашку» на странице урока (через GitHub `/generate`, codecrafters-pattern).
        2. Склонируй копию локально, реализуй TODO в `main.go`, прогони тесты, запушь.
        3. CI (`.github/workflows/ci.yml`) запускает `go vet` + `go test ./...` на каждый push. Платформа слушает результат через webhook от GitHub Actions и обновляет статус домашки на странице урока.

        ## Локальное окружение

        - Go 1.22+
        - Docker + docker-compose — `docker compose -f docker-compose.yml up -d` поднимает 3-нодовый Kafka cluster на портах 9092/9093/9094, использовать в тестах через bootstrap `localhost:9092,localhost:9093,localhost:9094`.

        ## Запуск

        ```bash
        # Поднять локальный Kafka
        docker compose up -d

        # Прогнать тесты (часть из них стартует свой ephemeral testcontainers cluster, часть использует docker-compose выше)
        go test ./...

        # Запустить main (печатает marker; замени stub на реализацию)
        go run .
        ```

        ## Заметка автора

        Это baseline-шаблон, сгенерированный платформой. Бизнес-сущность задачи (что конкретно реализовать в `main.go`, какие тесты сделать строгими) расширяется по ходу итераций — параллельно с углублением теории урока.
