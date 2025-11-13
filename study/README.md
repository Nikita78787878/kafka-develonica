### [1. Для чего нужен брокер сообщений](1_appointment_broker.md)

**Темы**:

- Синхронно vs асинхронно; слабая связанность; eventual consistency.
- Pub/Sub и Point-to-Point.
- «durable» сообщения, порядок, backpressure.

**Цель**: научиться типовым сценариям: события домена, уведомления, очередь задач

---

### [2. Kafka: устройство](2_architecture.md)

**Темы**:

- Broker/Topic/Partition/Offset, репликация, retention и log compaction.
- Порядок внутри партиции, ключи и стратегия partitioning.
- KRaft: что это и зачем знать.

**Цель**: ознакомление с архитектурой

---
### [3. Producer, Consumer](3_producer_and_consumer.md)

**Темы**:

- Producer(Идемпотентный продюсер, acks, linger/batch.size)
- Consumer(авто/ручной коммит оффсетов, consumer group)

**Цель**: ознакомление с настройками продюсеров и консьюмеров

---

### [4. Гарантии доставки и надёжность в Kafka](4_delivery_garantees.md)

**Темы**:

- Семантика доставки сообщений: At-most-once, At-least-once, Exactly-once.
- Идемпотентность, дедупликация.
- Повторы, retry политики, poison pill.

**Цель**: научиться проектировать потребителей без дублей и потерь

---

### [5. Сериализация и эволюция схем](5_serialization_and_schema_evolution.md)

**Темы**:

- JSON vs Avro vs Protobuf; компромисс «читаемость/размер/скорость».
- Версионирование событий; совместимость (backward/forward).
- Заголовки сообщений, трассировка (traceId, correlationId).

**Цель**: научиться выбирать формат и добавлять версии без поломок

---


### [6. Spring for Kafka](6_spring_for_kafka.md)

**Темы**:

- KafkaTemplate, @KafkaListener, контейнеры слушателей, конкуренция потоков.
- Error handling: SeekToCurrentErrorHandler/DefaultErrorHandler, DLT.
- Транзакции и EOS (exactly-once v2).

**Цель**: научиться создавать надежного потребителя

---

### [7. RabbitMQ: устройство](7_rabbitMQ.md)

**Темы**:

- Exchanges: direct, topic, fanout, headers
- Queues, bindings: создание очередей, связь с exchange, роль routing keys.
- Подтверждения и надежность: ack/nack/requeue.
- Prefetch (QoS).
- TTL, задержки сообщений, DLX, приоритетные очереди.

**Цель**: изучить RabbitMQ

---

### [7.1. Kafka vs RabbitMQ](7_1_Kafka_vs_RabbitMQ.md)

**Темы**:

- Сравнение: модель, порядок, хранение, маршрутизация, транзакции, сложность эксплуатации.
- Типовые кейсы: события домена и потоковая обработка (Kafka) vs маршрутизация задач и интеграции (RabbitMQ).

**Цель**: научиться выбирать и обосновывать выбор

---

### [8. Spring AMQP](8_spring_amqp.md)

**Темы**:

- RabbitTemplate, @RabbitListener, контейнеры, retry
- Publisher confirms и возвраты, транзакционные каналы, batching.

**Цель**: научиться создавать надежного потребителя

---

### [9. Kafka Connect](9_kafka_connect.md)

**Темы**:

- подключение внешних систем (source/sink), коннекторы, интеграции без кастомного кода.

**Цель**: изучить Kafka Connect

---