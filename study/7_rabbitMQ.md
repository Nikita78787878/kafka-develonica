#### RabbitMQ: устройство

---
`RabbitMQ` — это брокер сообщений, который реализует протокол `AMQP` (Advanced Message Queuing Protocol).

Основная задача `RabbitMQ`: приём, маршрутизация и доставка сообщений между приложениями.

В основе устройства `RabbitMQ` лежат следующие элементы:

---
**Virtual Host (vhost) — изоляция ресурсов**

Виртуальный хост — это логическое пространство в брокере, содержащее свои очереди, обменники, 
биндинги и пользователей. Он позволяет разделять приложения и окружения в одном брокере.

В Java при подключении указываем `vhost` в настройках ConnectionFactory.
Например:
```java
CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
connectionFactory.setUsername("guest");
connectionFactory.setPassword("guest");
connectionFactory.setVirtualHost("/app1"); // подключение к vhost /app1
```

---
**Queue (Очередь)**

Очередь хранит сообщения до обработки.
Она существует внутри определённого vhost.
Очередь хранит сообщения до того, как их заберёт consumer.
Работает по принципу `FIFO` (First In – First Out).
Может быть durable, exclusive, auto-delete.

В Java можно создать очередь через @Bean или программно через RabbitAdmin.
```java
@Bean
public Queue orderQueue() {
    return new Queue("order_events", true); // durable = true
}
```

---
**Exchange (Обменник) — маршрутизатор сообщений**

Обменник получает сообщение от producer и решает, в какие очереди его направить.
Сам обменник не хранит сообщения.

#### Типы обменников и логика маршрутизации:

| Тип         | Как работает                                                     | Пример сценария                                               | 
|-------------|------------------------------------------------------------------|---------------------------------------------------------------|
| Direct      | точное совпадение routing key                                    | Логирование: "error" → logs_error                             | 
| Topic       | шаблон routing key <br/>(* = одно слово, # = любое количество)   | События заказов: order.* → billing, order.created → warehouse | 
| Fanout      | игнорирует routing key, отправляет во все очереди                | Рассылка уведомлений SMS, email, push                         | 
| Headers     | маршрутизация по заголовкам сообщения                            | Отчёты: headers={"format":"pdf"} → pdf_reports                |         

Exchange живёт в `vhost`. К exchange привязываются очереди через bindings

Direct Exchange:
```java
@Bean
DirectExchange logsDirectExchange() {
    return new DirectExchange("logs_direct");
}
```
Topic Exchange:
```java
@Bean
TopicExchange ordersTopicExchange() {
    return new TopicExchange("orders_topic");
}
```
Fanout Exchange:
```java
@Bean
FanoutExchange notificationsFanoutExchange() {
    return new FanoutExchange("notifications_fanout");
}
```
Headers Exchange:
```java
@Bean
HeadersExchange reportsHeadersExchange() {
    return new HeadersExchange("reports_headers");
}
```
---
**Binding (Биндинг) — правила маршрутизации**

Биндинг — это связь `exchange ↔ queue`, задающая правило маршрутизации.
Может включать `routing key` (для Direct/Topic) или `headers` (для Headers exchange).

Пример:
```java
// Direct
@Bean
Binding bindingLogsError(Queue orderQueue, DirectExchange logsDirectExchange) {
    return BindingBuilder.bind(orderQueue).to(logsDirectExchange).with("error");
}

// Topic
@Bean
Binding bindingOrdersCreated(Queue orderQueue, TopicExchange ordersTopicExchange) {
    return BindingBuilder.bind(orderQueue).to(ordersTopicExchange).with("order.created");
}

// Fanout
@Bean
Binding bindingNotifications(Queue orderQueue, FanoutExchange notificationsFanoutExchange) {
    return BindingBuilder.bind(orderQueue).to(notificationsFanoutExchange);
}

// Headers
@Bean
Binding bindingPdfReports(Queue orderQueue, HeadersExchange reportsHeadersExchange) {
    return BindingBuilder.bind(orderQueue)
            .to(reportsHeadersExchange)
            .where("format").matches("pdf");
}

```

⚠️ Без биндинга сообщения не попадут в очередь (если не включён alternate exchange).

---
**Routing Key (Маршрутный ключ)**

Строка, указываемая producer при отправке сообщения.
Используется exchange для определения, куда направить сообщение:

`Direct` → точное совпадение

`Topic` → шаблонное совпадение

`Fanout` → игнорируется

`Headers` → используется заголовок

```java
@Autowired
private RabbitTemplate rabbitTemplate;

// Direct
rabbitTemplate.convertAndSend("logs_direct", "error", "DB failure");

// Topic
rabbitTemplate.convertAndSend("orders_topic", "order.created", "Order 123 created");

// Fanout (routing key игнорируется)
rabbitTemplate.convertAndSend("notifications_fanout", "", "System maintenance");

// Headers
MessageProperties props = new MessageProperties();
props.setHeader("format", "pdf");
Message message = new Message("Report content".getBytes(), props);
rabbitTemplate.send("reports_headers", "", message);
```
---
**Подтверждения и надежность: ack/nack/requeue**

- `ack` (Acknowledgement) — consumer подтверждает успешную обработку сообщения → сообщение удаляется из очереди.

- `nack` (Negative Acknowledgement) — consumer сигнализирует об ошибке:

    с `requeue=true` → сообщение возвращается в очередь;

    с `requeue=false` → сообщение отбрасывается (или попадает в DLX).

Пример (Java, Spring AMQP):
```java
@RabbitListener(queues = "order_events")
public void processOrder(Message message, Channel channel) throws IOException {
    try {
        // обработка
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    } catch (Exception ex) {
        channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
        // requeue=true → сообщение вернётся в очередь
    }
}
```
Если consumer «упал» без ack, `RabbitMQ` автоматически вернёт сообщение в очередь.

---
**Prefetch (QoS)**

`Prefetch` — ограничение числа сообщений, доставляемых одному consumer одновременно (Quality of Service).

Позволяет балансировать нагрузку.

Например, `prefetch=1` означает, что consumer получит только одно сообщение за раз и новое сообщение будет доставлено только после `ack`.

Пример настройки:
```java
@Bean
public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
        ConnectionFactory connectionFactory) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    factory.setConnectionFactory(connectionFactory);
    factory.setPrefetchCount(1); // одно сообщение на consumer
    return factory;
}
```
---
**TTL и задержки сообщений**

`TTL` (Time-To-Live) — ограничивает время жизни сообщений или очереди.

Можно задать на уровне очереди (x-message-ttl) или отдельного сообщения.

Задержки (delayed messages) — реализуются через плагины (rabbitmq_delayed_message_exchange) или через «прокси-очередь» с `TTL` + `DLX`.

Пример с `TTL` для очереди:
```java
@Bean
public Queue ttlQueue() {
    return QueueBuilder.durable("ttl_queue")
            .withArgument("x-message-ttl", 10000) // 10 секунд
            .build();
}
```
Пример `TTL` на уровне сообщения:
```java
MessageProperties props = new MessageProperties();
props.setExpiration("5000"); // 5 секунд
Message message = new Message("Hello".getBytes(), props);
rabbitTemplate.send("ttl_exchange", "key", message);
```

---
**DLX (Dead Letter Exchange)**

`Dead Letter Exchange` — обменник, куда попадают «невостребованные» сообщения:

- сообщение просрочено (`TTL` истёк),

- очередь переполнена,

- consumer отклонил сообщение (`nack` с `requeue=false`).

Пример:
```java
@Bean
public Queue mainQueue() {
    return QueueBuilder.durable("main_queue")
            .withArgument("x-dead-letter-exchange", "dlx_exchange")
            .withArgument("x-dead-letter-routing-key", "dead_key")
            .build();
}

@Bean
public Queue deadLetterQueue() {
    return new Queue("dead_queue", true);
}

@Bean
public DirectExchange dlxExchange() {
    return new DirectExchange("dlx_exchange");
}

@Bean
public Binding bindingDLX() {
    return BindingBuilder.bind(deadLetterQueue())
            .to(dlxExchange())
            .with("dead_key");
}
```
---
**Приоритетные очереди**

`RabbitMQ` поддерживает очереди с приоритетами сообщений.
Сообщения с более высоким приоритетом доставляются первыми.

Пример:
```java
@Bean
public Queue priorityQueue() {
    return QueueBuilder.durable("priority_queue")
            .withArgument("x-max-priority", 10) // приоритет от 0 до 10
            .build();
}

// Отправка с приоритетом
MessageProperties props = new MessageProperties();
props.setPriority(9);
Message msg = new Message("VIP order".getBytes(), props);
rabbitTemplate.send("priority_exchange", "order", msg);
```

---
**Логическая схема RabbitMQ**

          ┌─────────────────────────┐
          │        vhost /app1      │
          │ (изолирует ресурсы:     │
          │  queues, exchanges,     │
          │  bindings, users)       │
          └───────────┬─────────────┘
                      │
                      ▼
              ┌───────────────┐
              │  Producer     │
              │ (отправка     │
              │  сообщений    │
              │  с routing    │
              │  key/headers) │
              └───────┬───────┘
                      │
                      ▼
    ┌──────────────────────────────────────┐
    │               Exchange               │
    │ ┌───────────────┬─────────────────┐  │
    │ │ Direct        │ Topic           │  │
    │ │ (routing key) │ (шаблон key)    │  │
    │ ├───────────────┼─────────────────┤  │
    │ │ Fanout        │ Headers         │  │
    │ │ (все очереди) │ (по заголовкам) │  │
    │ └───────────────┴─────────────────┘  │
    └─────────────────┬────────────────────┘
                      │
              [Binding rules]
                      │
                      ▼
              ┌───────────────┐
              │   Queue(s)    │
              │ (хранение     │
              │  сообщений)   │
              └──────┬────────┘
                     │
                     ▼
              ┌───────────────┐
              │  Consumer(s)  │
              │ (обработка    │
              │  сообщений)   │
              └───────────────┘


Логика работы:

`Producer` отправляет сообщение → `Exchange` решает маршрут по типу и биндингам → Сообщение попадает в `Queue` → `Consumer` забирает и обрабатывает.

`vhost` изолирует ресурсы, `binding` определяет правила маршрутизации, `routing key` или `headers` указывают, куда попадёт сообщение.

---
### 🎯 Практика: Поднимаем RabbitMQ в Docker

Обновляем `docker-compose.yml` — добавляем `RabbitMQ` к уже существующей `Kafka`
```java
rabbitmq:
    image: rabbitmq:3-management
    container_name: rabbitmq_lab
    ports:
        - "5672:5672"   # AMQP
        - "15672:15672" # web UI
    environment:
        RABBITMQ_DEFAULT_USER: guest
        RABBITMQ_DEFAULT_PASS: guest
```
Запустить:
```java
cd Kafka_example/infra
docker compose up -d
docker ps
```
Открыть `UI`:
- RabbitMQ Management: http://localhost:15672 (guest/guest)
- Kafka UI: http://localhost:8080

---
### Вопросы

--- 
- Что такое `RabbitMQ` и какой протокол он реализует?
- Что такое vhost и зачем он нужен?
- Чем отличается очередь `Queue` от обменника `Exchange`?
- Какие типы `exchange` существуют и как они работают?
- Что такое `binding` и как оно используется для маршрутизации сообщений?
- Как `routing key` влияет на доставку сообщения в `Direct` и `Topic exchange`?
- Как работает `Fanout exchange` и в каких случаях его используют?
- Что такое `Headers exchange` и для чего используются заголовки сообщений?
- Как `consumer` получает сообщение и что происходит, если он временно недоступен?
- Какие меры обеспечивают надёжность доставки сообщений в `RabbitMQ`?
- Чем отличаются `ack`, `nack` и `reject`? В каких случаях сообщение может вернуться в очередь?
- Для чего используется параметр `prefetch` и как он влияет на балансировку нагрузки между consumers?
- Какие ситуации приводят к попаданию сообщения в `Dead Letter Exchange`(DLX)?

---
### Упражнения

---

1. Поднять `RabbitMQ` в `Docker`
2. Попробовать создавать топики и отправлять сообщения через `UI`
3. Попробовать создавать топики и отправлять сообщения через консоль

---

## 📚 Дополнительные материалы и ссылки

### 📖 **Официальная документация**
- [RabbitMQ](https://www.rabbitmq.com/docs) - Официальная документация `RabbitMQ`
- [RabbitMQ Tutorials](https://www.rabbitmq.com/tutorials/tutorial-one-java-stream) - официальные примеры
- [Exchange & Bindings](https://www.rabbitmq.com/tutorials/tutorial-five-java) - Справочник по `Exchange` и `Bindings`
- [RabbitMQ Video YouTube](https://youtu.be/i-Eh-NCa0Tk?si=WDML79i9lOB12mqD) - Видео-курс по `RabbitMQ` (YouTube)
- [Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms?) - подробное объяснение подтверждений сообщений и их роли в обеспечении надежности доставки.
- [Consumer Prefetch](https://www.rabbitmq.com/docs/consumer-prefetch?) - описание механизма ограничения числа сообщений, доставляемых одному consumer одновременно, для балансировки нагрузки.
- [At-Least-Once Dead Lettering](https://www.rabbitmq.com/blog/2022/03/29/at-least-once-dead-lettering?) - введение в концепцию "по крайней мере одно" мертвого письма, обеспечивающую надежную доставку сообщений в `Dead Letter Exchange`.
### 🔥 **Лучшие статьи по теме (2024-2025)**
- [RabbitMQ Settings](https://habr.com/ru/companies/otus/articles/928152/) - Тонкие настройки отправки сообщения в RabbitMQ
- [RabbitMQ & Kafka & NATS](https://habr.com/ru/articles/923046/) - `Kafka`, `RabbitMQ`, NATS в 2025

### 🛠 **Инструменты для разработки**
- [Spring AMQP](https://spring.io/projects/spring-amqp) - интеграция `RabbitMQ` с Java/Spring
- [RabbitMQ Management Plugin](https://www.rabbitmq.com/docs/management) - веб-интерфейс для администрирования
- [RabbitMQ Module](https://java.testcontainers.org/modules/rabbitmq/) - запуск RabbitMQ в тестах через `Docker`

### 🎯 **Практические примеры**
- [RabbitMQ Java Client Examples](https://github.com/rabbitmq/rabbitmq-tutorials/tree/main/java) - реализация `RabbitMQ Java Client Examples` (GitHub)
- [RabbitMQ & Spring Boot](https://www.baeldung.com/spring-amqp) - реализация `Spring Boot` + `RabbitMQ` пример (Baeldung)

### 📊 **Источники информации**
Материал курса основан на:
- Официальной документации RabbitMQ
- Практических статьях и кейсах компаний (Habr, Baeldung)
- Лучших практиках Java сообщества
- Актуальных тенденциях развития экосистемы в 2025 году

---











