#### Kafka Connect

---

`Kafka Connect` — инструмент для масштабируемой и надежной потоковой передачи данных между `Apache Kafka` и другими системами данных. Работа `Kafka Connect` основана на создании специальных коннекторов, перемещающих данные в `Kafka` или наборот.

Он работает по принципу «`источник` → `преобразование` → `Kafka` → `преобразование` → `приёмник`».

Основные преимущества `Kafka Connect`:
- Копирование данных (создание коннекторов и задач, управление ими) выполняется при помощи `REST-интерфейса`.
- Широкие возможности масштабируемости: изменение количества исполнителей, подключение новых коннекторов к имеющимся платформам, управление количеством запущенных задач.
- Автоматическое управление смещением данных при работе коннекторов.
- Широкий выбор уже существующих плагинов коннекторов, а также возможность создать свой для нужд вашей системы.
- Интеграция потоковой/пакетной обработки данных.

---
 **Kafka Connect состоит из четырёх ключевых элементов:**

🔹 `Source Connector` - читает данные из внешней системы и пишет в Kafka-топики.
   
Принцип работы:

1) `Kafka Connect` запускает коннектор.
2) Коннектор подключается к источнику данных. 
3) Данные считываются и преобразуются в сообщения Kafka. 
4) Отправляются в один или несколько топиков.

📌 Примеры внешних систем:

| Система                | Коннектор                 | Особенности                                                              | 
|------------------------|---------------------------|--------------------------------------------------------------------------|
| MySQL/PostgreSQL       | Debezium MySQL/Postgres   | CDC (Change Data Capture) для потоковой передачи изменений.              | 
| MongoDB                | Debezium MongoDB          | лог изменений коллекций для потоковой синхронизации.                     | 
| Files / S3             | FileStream / S3 Source    | подключение к новым файлам или к файлам, изменяемым во времени.          | 
| JMS / MQTT             | JMS Source / MQTT Source  | читает сообщения и превращает их в Kafka Record с сохранением порядка.   |
Ключевые параметры:

- `tasks.max` — количество параллельных задач.
- `topic.prefix` — префикс топиков Kafka.
- `database.*` — параметры подключения к базе.

Конфигурация (postgres-source.json):
```java
{
  "name": "postgres-source-connector",
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",   // количество параллельных задач для чтения данных.
  "database.hostname": "localhost",
  "database.port": "5432",
  "database.user": "postgres",
  "database.password": "password",
  "database.dbname": "mydb",
  "database.server.name": "pg_server",
  "table.include.list": "public.orders",    //список таблиц для отслеживания.
  "plugin.name": "pgoutput",    //стандартный логический декодер PostgreSQL для CDC.
  "slot.name": "debezium_slot",     //имя слота логической репликации PostgreSQL.
  "publication.name": "dbz_publication",    //имя публикации PostgreSQL, необходимое для CDC.
  "database.history.kafka.bootstrap.servers": "localhost:9092",
  "database.history.kafka.topic": "dbhistory.orders",    //топик для хранения истории изменений схем таблиц.
  "topic.prefix": "postgres"
}
```
Регистрация коннектора через REST API:
```java
curl -X POST -H "Content-Type: application/json" \
        --data @postgres-source.json \
http://localhost:8083/connectors
```

---
🔹 `Sink Connector` - читает данные из Kafka и записывает их во внешние системы.

Принцип работы:

1) `Kafka Connect` получает данные из заданного топика.
2) Коннектор преобразует сообщения в формат внешней системы.
3) Данные записываются в целевую систему.

📌 Примеры внешних систем:

| Система          | Коннектор          | Особенности                                                    | 
|------------------|--------------------|----------------------------------------------------------------|
| Elasticsearch    | Elasticsearch Sink | индексация сообщений.                                          | 
| PostgreSQL/MySQL | JDBC Sink          | сохранение в таблицы БД.                                       | 
| S3 / HDFS        | S3 / HDFS Sink     | запись в файлы/партиции с поддержкой временных меток.          | 
| Cassandra        | Cassandra Sink     | поддержка upsert (обновление существующих ключей) или вставки. |

Ключевые параметры:

- `topics` — список топиков Kafka.
- `connection.url` / `connection.user` / `connection.password` — параметры подключения. 
- `insert.mode` (для JDBC) — режим вставки (insert, upsert, update). 
- `batch.size` — размер пакета для записи. 
- `auto.create` / `auto.evolve` — автосоздание и эволюция таблиц.

Конфигурация (postgres-sink.json):
```java
{
  "name": "postgres-sink-connector",
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "tasks.max": "1",
  "topics": "pg_server.public.orders",
  "connection.url": "jdbc:postgresql://localhost:5432/mydb_target",
  "connection.user": "postgres",
  "connection.password": "password",
  "insert.mode": "upsert",
  "pk.mode": "record_key",   //определяет, как выбирается первичный ключ:
  "pk.fields": "order_id",   //поля, используемые как уникальный ключ
  "auto.create": "true",
  "auto.evolve": "true",
  "batch.size": "500",
  "table.name.format": "orders"
}
```
Регистрация Sink Connector:
```java
curl -X POST -H "Content-Type: application/json" \
  --data @postgres-sink.json \
  http://localhost:8083/connectors
```

---
🔹 `Single Message Transform (SMT)` - это небольшие, настраиваемые преобразования, которые применяются к каждому сообщению на лету между Kafka и внешней системой.

Позволяет модифицировать сообщение без изменения коннектора или кода.

📌 Примеры использования:

- Фильтрация полей: удалить ненужные колонки. 
- Переименование полей: привести имена к стандарту внешней системы. 
- Добавление статических или вычисляемых значений: например, добавление timestamp или source_system. 
- Изменение ключа сообщения для Sink Connector.

Пример конфигурации `SMT` для `Kafka Connect` (Можно вставить в конфигурацию `Sink Connector`):
```java
"transforms": "AddSourceField",
"transforms.AddSourceField.type": "org.apache.kafka.connect.transforms.InsertField$Value",
"transforms.AddSourceField.static.field": "source_system",
"transforms.AddSourceField.static.value": "postgres"
```
Как работает:
- Сообщение поступает из `Source Connector` или `Kafka`. 
- `SMT` применяет заданные преобразования. 
- Преобразованное сообщение отправляется в `Kafka` или во внешнюю систему.

❗ Для сложных бизнес-преобразований лучше использовать `Kafka Streams` или `ksqlDB`.

---
🔹`Clustered Mode` (Distributed Mode) - это режим работы `Kafka Connect`, где несколько воркеров объединяются в кластер, чтобы совместно обрабатывать коннекторы и задачи.

Отличается от `Standalone Mode`, где коннектор работает в одном процессе.

Принцип работы:
- Воркеры объединяются в кластер. 
- Коннектор регистрируется в `Kafka Connect REST API`. 
- Кластер распределяет задачи (tasks) коннектора между воркерами. 
- Если воркер выходит из строя, задачи перекладываются на доступные воркеры.

Конфигурация `Clustered Mode` (указывается в `connect-distributed.properties`):
```java
bootstrap.servers=localhost:9092
group.id=connect-cluster
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
config.storage.topic=connect-configs
offset.storage.topic=connect-offsets
status.storage.topic=connect-status
```
`config.storage.topic`, `offset.storage.topic`, `status.storage.topic` — системные топики Kafka для хранения конфигурации, смещений и состояния воркеров.

             ┌───────────────────────────┐
             │       Kafka Cluster       │
             │ (топики source/sink)      │
             └───────────────────────────┘
                        ▲
                        │
                ┌─────────────────┐
                │ Connect Cluster │
                │ (несколько      │
                │  воркеров)      │
                └─────────────────┘
                ▲       ▲       ▲
             Worker1  Worker2  Worker3
             │         │        │
        ┌─────────┐ ┌─────────┐ ┌─────────┐
        │ Task A1 │ │ Task A2 │ │ Task B1 │
        └─────────┘ └─────────┘ └─────────┘

---
**Схема работы Kafka Connect**


    [Внешняя система / Источник данных]
                     │
                     ▼
            ┌───────────────────┐
            │  Source Connector │
            │  (Debezium, File, │
            │   JMS и др.)      │
            └───────────────────┘
                     │
                     ▼
            ┌─────────────────────┐
            │       SMT           │  <- опционально
            │  (переименование,   │
            │   фильтрация,       │
            │   добавление полей) │
            └─────────────────────┘
                     │
                     ▼
                [Kafka Topic]
                     │
                     ▼
            ┌─────────────────────┐
            │       SMT           │  <- опционально
            │  (подготовка к Sink │
            │   или модификация)  │
            └─────────────────────┘
                    │
                    ▼
            ┌───────────────────┐
            │   Sink Connector  │
            │  (JDBC, S3,       │
            │   Elasticsearch,  │
            │   Cassandra и др.)│
            └───────────────────┘
                    │
                    ▼
    [Внешняя система / Приёмник данных]

1. `Source Connector` считывает данные из внешней системы. 
2. Опционально применяются `SMT`, чтобы изменить или подготовить данные. 
3. Данные отправляются в `Kafka-топик`. 
4. При чтении данных для `Sink Connector` снова могут применяться `SMT`. 
5. `Sink Connector` записывает данные во внешнюю систему.

---
🔹 **Когда использовать Kafka Connect?**

✅ Когда нужно:

- быстро интегрировать БД, файловые системы, API с Kafka;

- получать надёжный и поддерживаемый коннектор;

- строить пайплайны с минимумом кода.

❌ Когда НЕ подходит:

- если нужна сложная бизнес-логика или агрегации → лучше Kafka Streams или обычный Consumer API;

- если нет готового коннектора для твоей системы (хотя можно написать свой).

---
**Quick start Kafka Connect**

В отличие от внешних приложений, Kafka Connect не требует отдельной установки, она идёт вместе с Apache Kafka.

`Standalone Mode`:
```java
connect-standalone.sh config/connect-standalone.properties config/postgres-source.json config/postgres-sink.json
```

`Distributed Mode` (кластер):
```java
connect-distributed.sh config/connect-distributed.properties
```

Проверка коннекторов:
```java
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/postgres-source-connector/status
```

---
### Вопросы

--- 
- Для чего используется `Kafka Connect`?
- В каких случаях выгодно применять готовые коннекторы, а когда стоит писать свой?
- Что такое `Source Connector` и `Sink Connector`, приведи примеры систем, с которыми они работают.
- Какие преимущества `Kafka Connect` даёт по сравнению с «ручным» написанием продюсеров и консумеров?
- Какие типы внешних систем чаще всего интегрируются через `Kafka Connect`?
- Какие ограничения или слабые места у `Kafka Connect`?

---
### Упражнения

---
Нужно сделать функциональность для аналитики.

1. Этап 1: Проектирование базы данных для аналитики
Задача: Создать структуру БД, оптимизированную для аналитических запросов по заказам.
-- Основная таблица заказов, например:
CREATE TABLE orders_analytics (
id SERIAL PRIMARY KEY,
order_id VARCHAR(100) UNIQUE NOT NULL,
order_data JSONB,  -- Полные данные заказа
status VARCHAR(50),  -- 'processed', 'failed'
customer_email VARCHAR(255),
total_amount DECIMAL(10,2),
items_count INTEGER,
created_at TIMESTAMP,
processed_at TIMESTAMP,
failure_reason TEXT
);

-- Агрегированная таблица для быстрой аналитики, например:
CREATE TABLE daily_order_stats (
date DATE PRIMARY KEY,
total_orders INTEGER,
successful_orders INTEGER,
failed_orders INTEGER,
total_revenue DECIMAL(15,2),
avg_order_value DECIMAL(10,2)
);
2. Этап 2: Настройка Kafka Connect
План действий:
Установка и настройка Kafka Connect:
Добавить Kafka Connect в docker-compose
Установить JDBC connector для выбранной БД
3. Этап 3: Доработка Notification Service
   Задача: Научить Notification Service не только отправлять уведомления, но и обновлять аналитические данные.
   - Создание нового топика: orders.analytics
   - Настройка коннектора из этого топика в БД
   - Notification Service публикует в этот топик после отправки email(через kafka connect)

---

## 📚 Дополнительные материалы и ссылки

### 📖 **Официальная документация**
- [Kafka Connect](https://kafka.apache.org/documentation) - Официальная документация `Kafka Connect`
- [Confluent Documentation](https://docs.confluent.io/platform/current/connect/index.html) - Официальная документация Confluent, покрывающая работу коннекторов, REST API, примеры и справочники по настройке.
- [Understanding Kafka Connect](https://docs.lenses.io/latest/connectors/understanding-kafka-connect) - Хорошее пояснение архитектурных компонентов Connect, плагинов, воркеров и конвертеров.

### 🔥 **Лучшие статьи по теме (2024-2025)**
- [Kafka Connect](https://habr.com/ru/articles/809191/) - Как настроить Source коннекторы Kafka Connect для оптимизации пропускной способности
- [Kafka Connect & Postgres](https://gofunc.ru/talks/ca45f992d21a40a9afb63838e72df181/) - Kafka Connect, или Как передавать огромные потоки данных между базами

### 🛠 **Инструменты для разработки**
- [Confluent Hub](https://www.confluent.io/hub/) - библиотека готовых source/sink-коннекторов (Postgres, MySQL, S3, Elasticsearch, Cassandra и др.).
- [Debezium](https://debezium.io/) - веб-интерфейс для администрирования
- [Strimzi](https://strimzi.io/) - оператор для развертывания Kafka и Kafka Connect в Kubernetes.
  Позволяет управлять через CRD-ресурсы KafkaConnect, KafkaConnector.

### 🎯 **Практические примеры**
- [Kafka Connect at Scale: The Hidden Challenges](https://medium.com/%40rajkumar.rajaratnam/kafka-connect-at-scale-the-hidden-challenges-44bab174740d) - Практика масштабирования Kafka Connect: проблемы с наблюдаемостью, rebalancing, SMT, обработка ошибок и др
- [A Practical Guide to Kafka Connect](https://habr.com/ru/articles/751256/) - Ивентная модель данных с использованием Kafka и Kafka Connect: Построение гибкой и распределенной архитектуры

### 📊 **Источники информации**
Материал курса основан на:
- Официальной документации 
- Практических статьях и кейсах компаний 
- Лучших практиках Java сообщества
- Актуальных тенденциях развития экосистемы в 2025 году

---



















