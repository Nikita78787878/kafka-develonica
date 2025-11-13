#### Spring for Kafka

---
**🔹Продюсирование сообщений через KafkaTemplate**

В `Spring Kafka` отправка сообщений в топики строится вокруг бина `KafkaTemplate`.
Он поддерживает синхронную/асинхронную отправку, callback-обработку ошибок и работу в транзакциях.

Пример отправки сообщения:
```java
@Service
public class OrderProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(String orderId) {
        kafkaTemplate.send("orders", orderId, "Order created: " + orderId)
                .addCallback(
                        result -> System.out.println("✅ Sent: " + result.getRecordMetadata()),
                        ex -> System.err.println("❌ Failed: " + ex.getMessage())
                );
    }
}
```
💡 Благодаря `callback` мы можем вести мониторинг метрик и регистрировать ошибки при отправке.

---

**Подписка на сообщения: @KafkaListener и контейнеры слушателей**

Чтобы принимать сообщения, `Spring` предоставляет удобную аннотацию `@KafkaListener`.
Под капотом работает `ConcurrentMessageListenerContainer`, который управляет обработкой сообщений в потоках.

Пример слушателя:
```java
@Service
public class OrderConsumer {

    @KafkaListener(topics = "orders", groupId = "order-service")
    public void handleOrder(String message) {
        System.out.println("📥 Received: " + message);
    }
}
```
Если сообщений много, можно включить параллельную обработку через настройку `concurrency`:
```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
        ConsumerFactory<String, String> consumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setConcurrency(3); // количество потоков
    return factory;
}
```
💡 Так мы можем масштабировать обработку внутри одного `consumer group`.

---

**🔹 Error handling: SeekToCurrentErrorHandler / DefaultErrorHandler**

При ошибках обработки `Kafka` по умолчанию пытается перечитывать проблемное сообщение бесконечно.
Чтобы избежать «вечного зацикливания», в `Spring Kafka` используется `DefaultErrorHandler`, который позволяет:

- настроить число повторных попыток (retry);

- определить исключения, которые не стоит повторять;

- настроить отправку «плохих» сообщений в DLT.

В `Spring Kafka 2.8+` вместо `SeekToCurrentErrorHandler` используется `DefaultErrorHandler`.

```java
@Bean
public DefaultErrorHandler errorHandler() {
    FixedBackOff backOff = new FixedBackOff(1000L, 3); // 3 ретрая с шагом 1 сек
    var handler = new DefaultErrorHandler(
            (record, ex) -> System.err.println("💥 Failed record: " + record.value()),
            backOff
    );

    handler.addNotRetryableExceptions(IllegalArgumentException.class);
    return handler;
}
```
💡 Теперь сообщение будет пробовано несколько раз, а затем — передано в `Dead Letter Topic`.

---
**🔹 DLT (Dead Letter Topic)**

`Dead Letter Topic` — это топик для сообщений, которые `consumer` не смог корректно обработать даже после повторных попыток.

Spring Kafka упрощает интеграцию с помощью `DeadLetterPublishingRecoverer`:
```java
@Bean
public DeadLetterPublishingRecoverer publisher(KafkaTemplate<Object, Object> template) {
    return new DeadLetterPublishingRecoverer(
            template,
            (r, e) -> new TopicPartition(r.topic() + ".DLT", r.partition())
    );
}

@Bean
public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
    return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
}
```
Пример консьюмера для `DLT`:
```java
@KafkaListener(topics = "orders.DLT", groupId = "dlt-consumer")
public void handleFailedMessages(String msg) {
    System.err.println("🚨 From DLT: " + msg);
}
```
Таким образом, уходят в специальный топик для последующего анализа.

---
**🔹 Транзакции и Exactly-Once Semantics (EOS)**

Последний важный шаг — обеспечить атомарность обработки сообщений:
чтобы consumer прочитал сообщение, обработал его и отправил результат в `Kafka` (или в БД) в одной транзакции.

Для этого `Kafka` поддерживает идемпотентность и транзакции. В `Spring Kafka` достаточно включить их через `transactionIdPrefix`:
```java
@Bean
public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> pf) {
    KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
    template.setTransactionIdPrefix("tx-");
    return template;
}
```
Использование транзакций:
```java
@Service
public class TransactionalConsumer {

    private final KafkaTemplate<String, String> template;

    public TransactionalConsumer(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    @KafkaListener(topics = "orders", groupId = "transactional-service")
    @Transactional
    public void processOrder(String order) {
        System.out.println("⚙️ Processing: " + order);
        // бизнес-логика + запись в БД
        template.send("processed-orders", order);
    }
}
```
💡 Теперь фиксируются и `offset`, и новые записи в Kafka в рамках одной транзакции.
Это даёт гарантию `Exactly-Once Semantics (EOS v2)`: каждое сообщение будет обработано ровно один раз.

---
### Вопросы
- Что такое `KafkaTemplate` в `Spring` и какие режимы отправки сообщений он поддерживает?
- Как работает аннотация `@KafkaListener` и чем отличается от ручного создания `consumer`?
- Что такое `ConcurrentMessageListenerContainer` и как параметр `concurrency` влияет на обработку сообщений?
- Что произойдет, если установить `concurrency` больше, чем количество партиций в топике?
- В чём разница между `SeekToCurrentErrorHandler` и `DefaultErrorHandler` в `Spring Kafka`?
- Как настроить `Dead Letter Topic (DLT)` и зачем он используется?
- Какие типы исключений можно пометить как неподлежащие повторной обработке (not retryable) в error handler?
- Как в `Spring Kafka` реализовать транзакционный consumer, который гарантирует обработку сообщений ровно один раз?
- Что происходит с `offset` при использовании транзакций в `Spring Kafka`?
- Какие ограничения и подводные камни могут быть у использования транзакций и `Exactly-Once Semantics (EOS v2)` в продакшне?

---
### Упражнения

Необходимо переписать микросервисы `Order Gateway` и `Order Processor`, используя возможности `Spring Kafka`.
1. Создайте два `Spring Boot` модуля:
- `order-gateway-service` - продюсер сообщений;
- `order-processor-service` - консьюмер сообщений.
2. Добавьте зависимость `spring-kafka`.
3. В `order-gateway-service` создайте сервис `OrderProducer`:
- внедрите `KafkaTemplate<String, String>`
- метод `sendOrder(orderId, orderData)` должен отправлять сообщение в топик `orders.new`, логировать успех / ошибку через `callback`
4. В главном классе (`@SpringBootApplication`), реализуйте интерфейс `CommandLineRunner`. В методе `run()` создайте 
несколько примеров заказа (например `orderId = "order-1"`) и вызовите `sendOrder` с тестовыми данными.
5. В `order-processor-service` создайте сервис `OrderConsumer`:
- в методе `processOrders()` выбрасывайте ошибку, если сообщение не содержит `email`, или выведите в консоль сообщение об успешной обработке.
6. Добавьте класс `KafkaErrorConfig`, который будет обрабатывать ошибки с `DLT`.
7. Настройте обработку ошибок и `Dead Letter Topic (DLT)`:
- Создайте конфигурационный класс `KafkaErrorConfig`;
- Настройте `DefaultErrorHandler` и `DeadLetterPublishingRecoverer`;
- Создайте метод `handleFailedMessages()` с аннотацией `@KafkaListener(topics = "orders.new.DLT")`, чтобы принимать сообщения из `DLT` и выводить их в консоль.

**Дополнительные задания:**
1. Включите идемпотентность продюсера (`enable.idempotence=true`).
2. Попробуйте варианты `acks=0`, `acks=1` и `acks=all`и сравните надёжность доставки.
3. Настройте ручной коммит оффсета — `AckMode.MANUAL`, и вызывайте `ack.acknowledge()` после успешной обработки.
4. Отправьте сообщение с неверным форматом (например, без обязательного поля). Убедитесь, что оно после нескольких попыток попадает в `DLT`.
5. Настройте `KafkaTemplate` с `transactionIdPrefix` и добавьте `@Transactional` в `OrderConsumer`. Проверьте, что `offset` фиксируется только при успешной отправке результата.

---
## 📚 Дополнительные материалы и ссылки

### 📖 **Официальная документация**
- [Spring for Apache Kafka Reference](https://docs.spring.io/spring-kafka/reference/) - официальная документация Spring Kafka: продюсеры, консьюмеры, контейнеры слушателей, транзакции, обработка ошибок.
- [Spring Kafka API (Javadoc)](https://docs.spring.io/spring-kafka/api/) - JavaDoc по KafkaTemplate, @KafkaListener, контейнерам и обработчикам ошибок.

### 🔥 **Лучшие статьи по теме (2024-2025)**
- [Transactions in Apache Kafka with Spring Boot](https://www.confluent.io/blog/transactions-apache-kafka/) - как реализовать Exactly-Once Semantics (EOS) в Spring Kafka с транзакциями.
- [Error Handling Mechanism in Spring Kafka](https://medium.com/%40kaushikgopu1998/default-error-handling-mechanism-in-spring-kafka-0d6936e4f73c) - Статья о внутреннем устройстве DefaultErrorHandler, бэкофах, recoverers и исключениях
- [Building Kafka with Spring Boot on Kubernetes](https://medium.com/%40mustafaguc/building-kafka-producer-and-consumer-microservices-with-spring-boot-on-kubernetes-using-github-0bd0af37e538) - Пример построения микросервисов с Kafka и Spring Boot, CI/CD, деплой в Kubernetes
- [Implementing Kafka using Spring Boot](https://medium.com/ing-tech-romania/implementing-a-basic-kafka-producer-and-consumer-using-spring-boot-spring-kafka-and-avro-schema-2b6d06e6c4cf) - пример интеграции с Avro, Spring Kafka

### 🎯 **Практические примеры**
- [Building Kafka Using Spring Boot and Docker](https://www.solutiontoolkit.com/blog/spring-boot-kafka-producer-consumer-with-docker) - Руководство по созданию микросервисов Kafka Producer и Consumer с использованием Spring Boot и Docker.
- [Spring-boot-kafka-example](https://github.com/hennroja/spring-boot-kafka-example) - Репозиторий-пример интеграции Kafka в Spring Boot.

### 📊 **Источники информации**
Материал курса основан на:
- Практических статьях и кейсах компаний 
- Лучших практиках Java сообщества
- Актуальных тенденциях развития экосистемы в 2025 году
- Официальной документации и гайдах по Apache Kafka (Producer & Consumer)

---