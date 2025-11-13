### Сериализация и эволюция схем в Apache Kafka

---
**💡 Зачем нужна сериализация в Kafka?**

Apache Kafka работает с байтами. Все сообщения, которые отправляются и получаются из топиков, должны быть преобразованы в массивы байтов.
`Сериализация` — это процесс преобразования объектов Java в байты для передачи, а `десериализация` — обратный процесс.

**Основные задачи сериализации в Kafka:**

1. **Эффективность передачи** - минимизация размера сообщений для экономии сетевого трафика и дискового пространства
2. **Производительность** - быстрое преобразование объектов в байты и обратно
3. **Совместимость** - возможность изменения структуры данных без поломки существующих потребителей
4. **Типобезопасность** - контроль структуры данных на этапе компиляции или выполнения

```java
// Пример: Kafka работает с байтами
Producer<String, byte[]> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("my-topic", "key", messageBytes));
```

**🎯 Цель этой темы:** научиться выбирать оптимальный формат сериализации и управлять эволюцией схем данных без поломки системы.

---

**Сравнение форматов сериализации**

Рассмотрим три основных подхода к сериализации в Kafka-системах:

**1. JSON (JavaScript Object Notation)**

**Описание:** Текстовый формат обмена данными, основанный на синтаксисе JavaScript, но используемый независимо от языка программирования.

**Преимущества:**
- ✅ **Читаемость** - можно просматривать и редактировать вручную
- ✅ **Простота отладки** - легко понять содержимое сообщения
- ✅ **Универсальность** - поддержка всеми языками программирования
- ✅ **Гибкость** - добавление новых полей без поломки обратной совместимости
- ✅ **Нет необходимости в схеме** - самоописывающийся формат

**Недостатки:**
- ❌ **Размер** - избыточность из-за повторения имен полей
- ❌ **Производительность** - медленная сериализация/десериализация
- ❌ **Отсутствие валидации** - нет строгой схемы, ошибки выявляются во время выполнения
- ❌ **Типы данных** - ограниченная поддержка типов (строки, числа, булевы значения)

Пример JSON сообщения:
```json
{
  "userId": 12345,
  "username": "john_doe",
  "email": "john@example.com",
  "timestamp": "2025-01-15T10:30:00Z"
}
```
В Spring Kafka это выглядит:
```java
// Spring автоматически сериализует UserEvent в JSON
@Component
public class JsonUserEventProducer {
    
    @Autowired
    private KafkaTemplate<String, UserEvent> kafkaTemplate;
    
    public void sendUserEvent(UserEvent event) {
        kafkaTemplate.send("user-events", event.getUserId().toString(), event);
    }
}
```
Spring Boot автоматически подключает `JsonSerializer` и `JsonDeserializer`, которые используют библиотеку `Jackson`. Вы не пишете код сериализации — он работает «из коробки».

**2. Apache Avro**

**Описание:** Компактный бинарный формат сериализации данных, разработанный в рамках проекта `Apache Hadoop`.У него есть 
важное отличие: он описывает структуру данных в схеме (.avsc файл). Эта схема используется и при записи, и при чтении.

**Преимущества:**
- ✅ **Компактность** - эффективное использование пространства
- ✅ **Быстрая сериализация** - высокая производительность
- ✅ **Эволюция схем** - встроенная поддержка совместимости версий
- ✅ **Богатые типы данных** - поддержка сложных типов (union, enum, fixed)
- ✅ **Кодогенерация** - автоматическое создание классов из схем
- ✅ **Самоописание** - схема может быть встроена в файл

**Недостатки:**
- ❌ **Сложность** - требует понимания концепций схем
- ❌ **Нечитаемость** - бинарный формат, нельзя просмотреть вручную
- ❌ **Зависимость от схемы** - необходим Schema Registry для управления
- ❌ **Размер схемы** - схема может быть большой для простых сообщений

Пример Avro схемы (user-event.avsc):
```json
{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.example.events",
  "fields": [
    {"name": "userId", "type": "long"},
    {"name": "username", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
  ]
}
```
`Сообщение в Avro` — это просто байты, но Schema Registry хранит схему отдельно и гарантирует, что продюсер и консьюмер «говорят на одном языке».

Пример использования в Spring Kafka:
```java
@Component
public class AvroUserEventProducer {
    
    @Autowired
    private KafkaTemplate<String, UserEvent> kafkaTemplate;
    
    public void sendUserEvent(UserEvent event) {
        // Сериализатор автоматически отправит схему в Registry
        kafkaTemplate.send("user-events-avro", 
                          event.getUserId().toString(), 
                          event);
    }
}
```
>Чтобы это заработало, необходимо: 
> 1. Подключить зависимость `io.confluent:kafka-avro-serializer`
> 2. Указать URL Schema Registry в настройках
> 3. Иметь сгенерированные Java-классы

**3. Protocol Buffers (Protobuf)**

**Описание:** Бинарный формат сериализации, разработанный `Google`. Использует .proto файлы для определения структуры данных и генерирует код для различных языков программирования.

**Преимущества:**
- ✅ **Высокая производительность** - одна из самых быстрых сериализаций
- ✅ **Компактность** - очень эффективное использование места
- ✅ **Строгая типизация** - проверка типов на этапе компиляции
- ✅ **Обратная совместимость** - хорошая поддержка эволюции схем
- ✅ **Кроссплатформенность** - поддержка множества языков
- ✅ **Валидация** - автоматическая проверка структуры данных

**Недостатки:**
- ❌ **Сложность настройки** - требует компиляции .proto файлов
- ❌ **Нечитаемость** - бинарный формат
- ❌ **Менее гибкий** - изменения схемы требуют перекомпиляции
- ❌ **Зависимость от инструментов** - необходим protoc компилятор

Пример `Protobuf` схемы (user_event.proto):
```protobuf
syntax = "proto3";

package com.example.events;

option java_package = "com.example.events";
option java_outer_classname = "UserEventProtos";

message UserEvent {
  int64 user_id = 1;
  string username = 2;
  optional string email = 3;
  int64 timestamp = 4;
  EventType event_type = 5;
  
  enum EventType {
    CREATED = 0;
    UPDATED = 1;
    DELETED = 2;
  }
}
```
>Каждому полю присваивается номер (= 1, = 2), и именно по этим номерам происходит сериализация — не по именам!

**Сравнительная таблица форматов**

| Критерий | JSON | Apache Avro | Protocol Buffers |
|----------|------|-------------|------------------|
| **Размер сообщения** | ❌ Большой (100%) | ✅ Средний (30-50%) | ✅ Малый (20-40%) |
| **Скорость сериализации** | ❌ Медленная | ✅ Быстрая | ✅ Очень быстрая |
| **Читаемость** | ✅ Отличная | ❌ Нечитаемый | ❌ Нечитаемый |
| **Отладка** | ✅ Простая | ❌ Сложная | ❌ Сложная |
| **Эволюция схем** | ⚠️ Ограниченная | ✅ Отличная | ✅ Хорошая |
| **Типобезопасность** | ❌ Слабая | ✅ Строгая | ✅ Очень строгая |
| **Настройка** | ✅ Простая | ⚠️ Средняя | ❌ Сложная |
| **Экосистема** | ✅ Универсальная | ⚠️ Hadoop-ориентированная | ✅ Google-ориентированная |
| **Schema Registry** | ❌ Не требуется | ✅ Рекомендуется | ⚠️ Опционально |

**Рекомендации по выбору формата**

**🎯 Выбирайте JSON когда:**
- Прототипирование и разработка
- Небольшой объем данных (< 1MB/сек)
- Важна простота отладки
- Команда не готова к сложности Schema Registry
- Интеграция с внешними системами, которые ожидают JSON

**🎯 Выбирайте Apache Avro когда:**
- Высокая пропускная способность (> 10MB/сек)
- Важна эволюция схем и обратная совместимость
- Используете Confluent Platform или Schema Registry
- Данные хранятся длительно (Data Lake, аналитика)
- Команда готова инвестировать в Schema Registry

**🎯 Выбирайте Protocol Buffers когда:**
- Критична производительность и размер сообщений
- Микросервисная архитектура с gRPC
- Строгая типизация важнее гибкости
- Используете Google Cloud Platform
- Готовы к сложности настройки CI/CD для компиляции схем

---

**Версионирование событий и совместимость**

Одна из главных проблем в распределенных системах — это эволюция данных. Когда ваша система растет, структура событий неизбежно изменяется. Важно понимать, как обеспечить совместимость между различными версиями схем.

**Типы совместимости** 

| Тип                                                           | Что означает?                                                                                                                                                                                    | 
|---------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Backward Compatibility <br/>(Обратная совместимость)**      | Новая схема может читать старые данные. (Новые консьюмеры → старые сообщения)                                                                                                                    | 
| **Forward Compatibility <br/>(Прямая совместимость)**         | Старая схема может читать новые данные. (Старые консьюмеры → новые сообщения)<br/>т.е. новая схема записывает данные с доп. полем, а старая схема будет игнорировать неизвестные поля при чтении |
| **Full Compatibility <br/>(Полная совместимость)**            | Комбинация backward и forward совместимости                                                                                                                                                      |

**Правила совместимости в Avro**

**✅ Безопасные изменения (не нарушают совместимость):**
- Добавление поля с значением по умолчанию
- Удаление поля с значением по умолчанию
- Изменение значения по умолчанию для поля
- Добавление значения в enum (только в конец)
- Изменение документации

**❌ Опасные изменения (нарушают совместимость):**
- Удаление поля без значения по умолчанию
- Изменение типа поля
- Изменение имени поля без aliases
- Удаление значения из enum
- Изменение порядка полей в record

Пример эволюции схемы Avro:
```json
// v1.0
{
  "type": "record",
  "name": "OrderEvent",
  "fields": [
    {"name": "orderId", "type": "long"},
    {"name": "customerId", "type": "long"},
    {"name": "amount", "type": "double"}
  ]
}

// v2.0 - Добавляем новые поля с default значениями
{
  "type": "record", 
  "name": "OrderEvent",
  "fields": [
    {"name": "orderId", "type": "long"},
    {"name": "customerId", "type": "long"}, 
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string", "default": "USD"}, // Новое поле
    {"name": "timestamp", "type": "long", "default": 0},      // Новое поле
  ]
}
```

---

**Schema Registry**

`Schema Registry` — это централизованный сервис для управления схемами данных в Kafka. Он обеспечивает версионирование
схем, проверку совместимости и централизованное хранение схем, позволяет использовать ID схемы вместо самой схемы в каждом сообщении.

**Как это работает?**
1. Продюсер отправляет сообщение → сериализатор обращается к Registry.
2. Registry возвращает ID схемы → он записывается в заголовок сообщения.
3. Консьюмер получает сообщение → по ID загружает схему из Registry → десериализует.

Запуск локально: чтобы протестировать Avro-сообщения локально, достаточно запустить Kafka и Schema Registry вместе:
```yaml
# docker-compose.yml
version: '3.8'
services:
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
      SCHEMA_REGISTRY_DEBUG: 'true'
    ports:
      - "8081:8081"
    depends_on:
      - kafka
```
После запуска (docker-compose up) Schema Registry будет доступен по адресу http://localhost:8081.
Он автоматически создаёт топик _schemas для хранения схем.

**🔹 Настройка в Spring Boot**

Spring Boot может работать с Avro без написания кастомных сериализаторов, если указать нужные классы и URL Registry:

```yaml
# application.yml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      properties:
        schema.registry.url: http://localhost:8081
        auto.register.schemas: false  # Запрещаем автоматическую регистрацию
        use.latest.version: true      # Используем последнюю версию схемы
    
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      properties:
        schema.registry.url: http://localhost:8081
        specific.avro.reader: true    # Используем сгенерированные классы
        auto.offset.reset: earliest
```
- `auto.register.schemas: false` — схема должна быть зарегистрирована заранее (например, в CI). Это предотвращает случайные несовместимые изменения.
- `specific.avro.reader: true` — Spring будет использовать ваш сгенерированный класс (например, UserEvent), а не универсальный GenericRecord.

**🔹 Ручная работа со схемами**

В большинстве приложений не нужно управлять схемами из кода — это задача инфраструктуры.
Но если вы пишете инструмент для миграции или валидации, можно использовать клиент напрямую:

```java
@Service
public class SchemaValidationService {

    private final SchemaRegistryClient client =
            new CachedSchemaRegistryClient("http://localhost:8081", 10);

    public boolean isSchemaCompatible(String topic, Schema newSchema) {
        return client.testCompatibility(topic + "-value", newSchema);
    }
}
```
---

**Заголовки сообщений и трассировка**

Заголовки (`headers`) в Kafka — это метаданные, которые передаются вместе с сообщением, но не являются частью его содержимого. Они идеально подходят для трассировки, маршрутизации и передачи технической информации.

**🔹 Основные применения заголовков**

**1. Трассировка запросов (Distributed Tracing)**

```java
public void sendWithTracing(String topic, Object message) {
    ProducerRecord<String, Object> record = new ProducerRecord<>(topic, message);

    // Получаем traceId из текущего контекста
    String traceId = MDC.get("traceId");

    if (traceId != null) {
        record.headers().add("traceId", traceId.getBytes(StandardCharsets.UTF_8));
    }

    // Добавляем технические метаданные
    record.headers().add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
    record.headers().add("source", "user-service".getBytes());
    record.headers().add("version", "1.0".getBytes());
       
    kafkaTemplate.send(record);
}
```

**2. Обработка заголовков в Consumer**

```java
@Component
public class TracingKafkaConsumer {

    @KafkaListener(topics = "user-events")
    public void handleUserEvent(
            @Payload UserEvent event,
            @Header Map<String, Object> headers,
            ConsumerRecord<String, UserEvent> record) {
        
        // Извлекаем трассировочную информацию
        String traceId = new String((byte[]) headers.get("traceId"), StandardCharsets.UTF_8);

        // Устанавливаем контекст трассировки
        MDC.put("traceId", traceId);
        log.info("Processing event with traceId={}", traceId);
    }
    
}
```

**3. Интеграция с Spring Cloud Sleuth**
 Для работы необходимо добавить зависимости `spring-cloud-starter-sleuth`, `spring-cloud-sleuth-kafka`

Spring Cloud Sleuth автоматически добавляет traceId и spanId в заголовки Kafka:
```java
@NewSpan("user-created")  // Создает новый span
public void createUser(@SpanTag("userId") Long userId, String username) {
    UserEvent event = UserEvent.builder()
            .userId(userId)
            .username(username)
            .eventType(EventType.CREATED)
            .timestamp(System.currentTimeMillis())
            .build();

    // Sleuth автоматически добавит трассировочные заголовки
    kafkaTemplate.send("user-events", userId.toString(), event);
}

```

---

**Практические примеры с Spring Kafka**

**🔹 Пример 1: Многоформатная система с выбором сериализации**

В реальных микросервисных системах невозможно сразу перейти на единый формат данных.

Например:
- старые сервисы публикуют события в JSON
- новые — уже используют Avro
- внешние интеграции всё ещё ожидают только JSON.

Чтобы гибко работать с такими случаями, можно создать несколько `KafkaTemplate` — по одному на каждый формат сериализации.
Это позволит публиковать события в нужный топик с подходящим форматом, не ломая совместимость.
```java
@Configuration
public class MultiFormatKafkaConfig {

    // шаблон для JSON
    @Bean("jsonKafkaTemplate")
    public KafkaTemplate<String, Object> jsonKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        // здесь добавление конфигов в props        
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    // шаблон для Avro
    @Bean("avroKafkaTemplate")
    public KafkaTemplate<String, Object> avroKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        // здесь добавление конфигов в props        
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }
}
```

**🔹 Пример 2: Система с автоматическим fallback**

Даже хорошо настроенные системы иногда ломаются: `Schema Registry` может быть временно недоступен, или новая версия схемы окажется несовместимой.
В таких случаях лучше не терять событие, а корректно обработать сбой — например, сохранить в JSON, уведомить команду, и продолжить работу.
```java
@Component
public class ResilientEventProducer {

    @Autowired
    private KafkaTemplate<String, Object> primaryTemplate;
    
    @Autowired
    private KafkaTemplate<String, Object> fallbackTemplate;

    @Retryable(value = {Exception.class}, maxAttempts = 3, backoff = @Backoff(delay = 1000))
    public void sendEvent(String topic, Object event) {
        try {
            // Пытаемся отправить в основном формате (Avro)
            primaryTemplate.send(topic + "-avro", event).get(5, TimeUnit.SECONDS);
            log.info("Event sent successfully in Avro format");
            
        } catch (Exception e) {
            log.warn("Failed to send in Avro format, falling back to JSON", e);
            // Fallback на JSON
            fallbackTemplate.send(topic + "-json", event);
        }
    }
}
```

---

**Лучшие практики безопасности и производительности**

**1. Безопасность схем**

`Schema Registry` может содержать чувствительные данные (например, структуру внутренних событий).
Чтобы защитить доступ:
- Используйте Basic Auth или OAuth2 для аутентификации.
- Настраивайте SSL/TLS между сервисами и Schema Registry.
- Не храните пароли в коде — выносите их в application.yml или Vault.
- Ограничивайте доступ только нужным сервисам.

В Spring Boot можно настроить защищённое подключение, задав параметры:
```yaml
schema.registry:
  url: https://schema-registry.internal:8081
  auth:
    username: service-user
    password: ${SCHEMA_REGISTRY_PASSWORD}
  ssl:
    truststore: classpath:truststore.jks
    keystore: classpath:keystore.jks
```
Spring автоматически подставит их при создании клиента.

**2. Мониторинг и метрики**

Следите за метриками `Kafka` и сериализации:
- время сериализации / десериализации (latency);
- количество ошибок;
- количество отправленных / потреблённых сообщений;
- нагрузка на продюсеров и консьюмеров.

Используйте Micrometer и Prometheus/Grafana, чтобы визуализировать производительность и находить узкие места.

Пример метрик:
- `kafka.producer.record-send-rate`
- `kafka.consumer.records-lag-max`
- `kafka.serialization.error.count`

---

**Legacy vs 2025**

**Legacy подход:**
- Использование только JSON без Schema Registry
- Отсутствие версионирования схем
- Игнорирование заголовков сообщений
- Ручная обработка совместимости
- Отсутствие трассировки между сервисами

**Современный подход 2025:**
- **Выбор формата** основан на требованиях производительности и совместимости
- **Schema Registry** как центральный компонент управления схемами
- **Автоматизированная проверка** совместимости в CI/CD
- **Распределенная трассировка** через заголовки и OpenTelemetry
- **Мониторинг** метрик сериализации и производительности
- **Безопасность** схем через аутентификацию и шифрование

---

### Вопросы для самопроверки

1. В чем основное различие между `JSON`, `Avro` и `Protobuf` с точки зрения производительности?
2. Что такое `backward` и `forward` совместимость схем? Приведите примеры.
3. Как `Schema Registry` помогает в управлении эволюцией схем?
4. Для чего используются заголовки сообщений в `Kafka`?
5. Какие метрики важно мониторить при работе с различными форматами сериализации?

---

### Упражнения

1. **Создайте Avro схему** для события "OrderPlaced" с полями: orderId, customerId, items (массив), totalAmount, timestamp. Добавьте новую версию схемы с полем "discount" и проверьте обратную совместимость.

2. **Настройте Spring Kafka** для работы с тремя форматами сериализации (JSON, Avro, Protobuf) и реализуйте переключение между ними на основе конфигурации.

3. **Реализуйте трассировку** сообщений через несколько микросервисов, используя заголовки correlationId и traceId.

4. **Создайте систему мониторинга** для отслеживания метрик сериализации: время обработки, размер сообщений, количество ошибок.

---

## Практика


🎯 Задание: Реализация кастомных сериализаторов (JSON + Protobuf)
Цель:
Реализовать два кастомных сериализатора для разных сервисов:
JSON сериализатор - для Order Gateway (человеко-читаемый формат)
Protobuf сериализатор - для Order Processor (бинарный, эффективный)

📋 Шаг 1: Подготовка зависимостей
pom.xml - добавляем зависимости:
<!-- Kafka --> kafka-clients,
<!-- Protobuf --> protobuf-java
<!-- JSON Processing --> jackson-databind


📋 Шаг 2: Определение моделей данных
2.1 Создаем Protobuf схему
src/main/proto/order.proto:
message Order {
order_id;
email;
OrderItem items;
timestamp;
status;
processed_at;
}
message OrderItem {
id;
quantity;
name;
price;
}
message ProcessedOrder {
order_id;
original_data;
status;
processed_at;
failure_reason;
}
Компилируем: mvn compile - сгенерирует Java-классы

2.2 Сгенерировать Java POJO для JSON  для Order, OrderItem, ProcessedOrder
Notification сервис.

📋 Шаг 3: Реализация кастомного JSON сериализатора, используя ObjectMapper
3.1 JsonSerializer для Order Gateway
CustomJsonSerializer.java:

public class CustomJsonSerializer<T> implements Serializer<T> {
private final ObjectMapper objectMapper = new ObjectMapper();
}

3.2 JsonDeserializer для Order Processor
CustomJsonDeserializer.java:
public class CustomJsonDeserializer<T> implements Deserializer<T> {
private final ObjectMapper objectMapper = new ObjectMapper();

📋 Шаг 4: Реализация Protobuf сериализатора
4.1 ProtobufSerializer для Order Processor
ProtobufSerializer.java:
public class ProtobufSerializer<T extends com.google.protobuf.GeneratedMessageV3>
implements Serializer<T> {
}

4.2 ProtobufDeserializer для Notification Service
public class ProtobufDeserializer<T> implements Deserializer<T> {
```java```
}

📋 Шаг 5: Модификация сервисов
5.1 Order Gateway Service (использует JSON)
Сервис отправляет сообщения в формате JSON.
OrderGatewayService.java:
java
public class OrderGatewayService {
private static final String TOPIC = "orders.new";
}

5.2 Order Processor Service (JSON → Protobuf)
Сервис принимает сообщения в формате JSON и отправляет дальше(Notification Service)
сообщения в формате Protobuf
OrderProcessorService.java

5.3 Notification Service (читает Protobuf)
NotificationService.java

📋 Шаг 6: Тестирование и сравнение
6.1 Запуск системы:
bash
# 1. Запуск Kafka
docker-compose up -d

# 2. Создание топиков(если таковых ещё нет)
kafka-topics --create --topic orders.new --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic orders.processed --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic orders.failed --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 3. Запуск сервисов (в разных терминалах)
java -cp target/classes NotificationService
java -cp target/classes OrderProcessorService  
java -cp target/classes OrderGatewayService

В код сервисов можно добавить логгирование приема и отправки сообщений.
Тогда можно будет посмотреть исходящиие и входящие сообщения между сервисами.

---

## 📚 Полезные ссылки и материалы

### 📖 **Официальная документация**
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/) - Полная документация Apache Kafka
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) - Документация Schema Registry
- [Apache Avro Specification](https://avro.apache.org/docs/++version++/specification/) - Спецификация Apache Avro
- [Protocol Buffers Documentation](https://protobuf.dev/) - Документация Protocol Buffers

### 🔥 **Лучшие статьи с Хабра (2024-2025)**
- [Знакомимся с архитектурой Apache Kafka](https://habr.com/ru/companies/otus/articles/725168/) - Подробный разбор архитектуры Kafka
- [Туториал kafka + springboot + docker](https://habr.com/ru/articles/944672/) - Практическое руководство по интеграции
- [Кафка: преимущества и на что ещё обратить внимание при тестировании](https://habr.com/ru/companies/reksoft/articles/911132/) - Тестирование Kafka-систем
- [Apache Avro — на светлой стороне Кафки](https://habr.com/ru/companies/vsk_insurance/articles/843070/) - Подробный разбор Apache Avro
- [Разбираемся в Apache Kafka: подборка полезных статей и кейсов](https://habr.com/ru/companies/slurm/articles/683168/) - Коллекция полезных материалов

### 🛠 **Инструменты разработки**
- [Confluent Platform](https://www.confluent.io/platform/) - Коммерческая платформа на базе Kafka
- [Schema Registry UI](https://github.com/lensesio/schema-registry-ui) - Web интерфейс для Schema Registry
- [Kafka Tool](https://www.kafkatool.com/) - GUI клиент для Kafka
- [ksqlDB](https://ksqldb.io/) - Потоковая база данных для Kafka

### 🎯 **Практические примеры**
- [Spring Kafka Examples](https://github.com/spring-projects/spring-kafka/tree/main/spring-kafka-sample) - Официальные примеры Spring Kafka
- [Confluent Examples](https://github.com/confluentinc/examples) - Примеры использования Confluent Platform

### 📘 **Дополнительные материалы по сериализации**
- [Как работает Apache Avro в Kafka, Hadoop и других системах Big Data](https://bigdataschool.ru/blog/kafka-big-data-apache-avro.html) - Подробный разбор Apache Avro