### Spring AMQP

---

`Spring AMQP` — это проект, который упрощает работу приложений на Java с брокером сообщений RabbitMQ. Он предоставляет
удобные абстракции и механизмы для интеграции, аналогично тому, как Spring Kafka облегчает взаимодействие с Kafka. В
основе `Spring AMQP` лежат две ключевые составляющие:

- `RabbitTemplate`
- `MessageListenerContainer`

---

**RabbitTemplate, @RabbitListener, контейнеры, retry**

`RabbitTemplate` — это класс Spring для взаимодействия с `RabbitMQ`. Он предоставляет методы для отправки и получения
сообщений без необходимости вручную управлять соединением или каналами. Через @Autowired Spring внедряет готовый
экземпляр `RabbitTemplate`, настроенный в конфигурации приложения.

`MessageListenerContainer` (в т.ч. `SimpleMessageListenerContainer` и `DirectMessageListenerContainer`) — контейнеры, которые
получают сообщения от RabbitMQ и делегируют их в обработчики, управлят подключениями, потоками и подтверждениями.
`@RabbitListener` — декларативный способ привязать метод к очереди. Он создаёт и конфигурирует контейнер автоматически.

- `SimpleMessageListenerContainer` — традиционный контейнер, поддерживает concurrency и масштабирование через
  `concurrentConsumers`/`maxConcurrentConsumers`.

- `DirectMessageListenerContainer` — более современный контейнер, даёт лучший контроль над `consumer lifecycle` и зачастую
  лучше по производительности при больших нагрузках.

Пример конфигурационного класса с необходимыми бинами
```java
@Configuration
public class RabbitConfig {

    static final String queueName = "myQueue";

    @Bean
    Queue queue() {
        return new Queue(queueName, false);
    }

    @Bean
    RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        return new RabbitTemplate(connectionFactory);
    }

    @Bean
    SimpleMessageListenerContainer container(ConnectionFactory connectionFactory,
                                             MessageListenerAdapter listenerAdapter) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(queueName);
        container.setMessageListener(listenerAdapter);
        return container;
    }

    @Bean
    MessageListenerAdapter listenerAdapter(Receiver receiver) {
        return new MessageListenerAdapter(receiver, "receiveMessage");
    }
}
```
Компонент, который будет отправлять сообщения

```java
@Component
public class Sender {

    private final RabbitTemplate rabbitTemplate;

    @Autowired
    public Sender(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void send(String message) {
        rabbitTemplate.convertAndSend(RabbitConfig.queueName, message);
        System.out.println("Sent: " + message);
    }
}
```
Компонент, который будет обрабатывать входящие сообщения:

```java

@Component
public class Receiver {

    public void receiveMessage(String message) {
        System.out.println("Received: " + message);
    }
}
```

С аннтоацией `@RabbitListener` можно декларативно указать методы, которые будут обрабатывать сообщения из очередей

```java

@Service
public class MessageListener {

    @RabbitListener(queues = RabbitConfig.queueName)
    public void processMessage(String message) {
        System.out.println("Received message: " + message);
    }
}
```

Сообщения могут не обработаться из-за временных сбоев (внешняя БД, сеть). Spring AMQP поддерживает несколько решений от
сбоев:

- `Retry`-политику — автоматическое повторение обработки сообщения с задержкой
- `Recovery` механизмы — если сообщение не удалось обработать даже после всех ретраев, его можно перенаправить в `DLQ` (Dead
  Letter Queue).

Пример настройки `retry` в `application.yml`:

```yaml
spring:
  rabbitmq:
    listener:
      simple:
        retry:
          enabled: true
          max-attempts: 3
          initial-interval: 1000
          multiplier: 2
          max-interval: 10000

```

⚠️ Сообщение будет пробоваться 3 раза (с экспоненциальной паузой). Если после этого не обработалось — автоматически
попадёт в `DLQ` (main.queue.dlq).

---

### Publisher confirms и возвраты, транзакционные каналы

Когда продюсер отправляет сообщение, он хочет знать действительно ли брокер получил сообщение и удалось ли его
маршрутизовать в очередь. RabbitMQ даёт два механизма:

- `Publisher Confirms` — брокер подтверждает, что сообщение получено и записано в очередь.
- `Returns Callback` — вызывается, если сообщение не удалось маршрутизировать (например, не было подходящей очереди) и
  сообщение "возвращается" продюсеру.

Когда отправитель открывает канал для брокера очередей, он может использовать этот же канал для передачи подтверждений.
Теперь в ответ на полученное сообщение брокер очередей должен предоставить одно из двух:

- `basic.ack`. Положительное подтверждение. Сообщение получено, ответственность за него теперь лежит на RabbitMQ
- `basic.nack`. Негативное подтверждение. Что-то случилось, и сообщение не было обработано. Ответственность за него
  остаётся на источнике. При желании, он может отправить сообщение вторично

В дополнение к положительным и отрицательным уведомлениям о доставке сообщения предусмотрено сообщение `basic.return`.
Иногда отправителю нужно знать не только о том, что сообщение поступило в RabbitMQ, но и о том, что оно действительно
попало в одну или несколько очередей. Может так получиться, что источник отправляет сообщение в систему распределения по
очередям (`topic exchange`), в которой сообщение не маршрутизируется в ни одну из очередей доставки. В такой ситуации
брокер просто отбрасывает сообщение. В одних сценариях это нормально, в других же источник должен знать, было ли
сообщение сброшено, и действовать дальше в соответствии с этим.

Можно выставить флаг `“mandatory=true”` для отдельных сообщений, и, если сообщение не было определено в какую-либо очередь
доставки, отправителю будет возвращено сообщение `basic.return`. В этом случае брокер не отказывает, он по-прежнему
подтверждает приём, но в дополнение к этому возвращает сообщение в отдельном фрейме basic.return. Если на эти фреймы не
реагировать, смысл флага теряется, поэтому при его использовании клиент должен уметь обрабатывать возвраты — например,
публиковать сообщение в другой обменник.

```java
@Configuration
public class RabbitConfirmConfig {

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);

        // Включаем mandatory, чтобы получать возвраты
        template.setMandatory(true);

        // Callback для Publisher Confirms
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (ack) {
                System.out.println("✅ Сообщение доставлено в брокер: " + correlationData);
            } else {
                System.err.println("❌ Ошибка доставки в брокер: " + cause);
            }
        });

        // Callback для Returns
        template.setReturnsCallback(returned -> {
            System.err.println("⚠️ Сообщение не было маршрутизировано!");
            System.err.println("   ReplyCode: " + returned.getReplyCode());
            System.err.println("   ReplyText: " + returned.getReplyText());
            System.err.println("   Exchange: " + returned.getExchange());
            System.err.println("   RoutingKey: " + returned.getRoutingKey());
            System.err.println("   Сообщение: " + new String(returned.getMessage().getBody()));
        });

        return template;
    }
}
```

**Транзакционные каналы**

AMQP поддерживает транзакции на уровне канала: можно обернуть отправку/получение сообщений в
`txSelect`/`txCommit`/`txRollback`.

➕ Плюсы: даёт ACID-подобное поведение: гарантирует, что сообщение либо точно записано, либо откатилось.

➖ Минусы: очень тяжёлый механизм — каждая операция ждёт ответа брокера, а `commit`/`rollback` блокируют канал. На высоких
нагрузках пропускная способность может падать в десятки раз.

В продакшене чаще не применяются, вместо этого рекомендуют `Publisher Confirms` (намного быстрее и надёжнее).

Транзакции можно рассматривать разве что в узких сценариях (например, «очень маленький throughput + критичная финансовая
операция»).

---

**Batching сообщений**

В обычном режиме каждое сообщение отправляется и подтверждается отдельно. Это создаёт накладные расходы: на каждый
запрос нужно открыть соединение, дождаться ответа, сделать подтверждение.

Если сообщений много (сотни или тысячи в секунду), то работа «по одному» превращается в тормоз.

✅ Решение: сообщения можно собирать в группы (пакеты) и отправлять их сразу целиком.
Так уменьшается количество сетевых операций и повышается пропускная способность.

**Есть два уровня «пакетной» работы:**

1. На стороне отправителя (`Publisher batching`)

Отправитель (продюсер) не шлёт каждое сообщение отдельно. Вместо этого `Spring AMQP` позволяет накапливать их в буфере 
(`BatchingRabbitTemplate`). Как только выполняется одно из условий:

- достигнут размер пакета (например, 100 сообщений),
- или суммарный размер по байтам,
- или прошло заданное время ожидания (таймер),

все накопленные сообщения отправляются сразу одной порцией.

2. На стороне получателя (`Consumer batching`)

Получатель (консьюмер) может не обрабатывать каждое сообщение отдельно. Вместо этого контейнер слушателя отдаёт сразу
список сообщений (например, по 10 штук за раз). Тогда подтверждение (`ack`) отправляется не за каждое сообщение,
а сразу за всю пачку. Это экономит ресурсы, если каждое отдельное подтверждение было бы слишком дорогим.

❗Подводные камни:

- Задержки (`latency`): сообщение может подождать, пока наберётся пакет. Если система должна работать с минимальными
  задержками (реальное время), `batching` не подойдёт.
- Ошибки в пакете: если внутри батча одно сообщение «плохое» (например, не удалось обработать), нужно решить откатывать
  весь пакет, или попытаться обработать только проблемное сообщение отдельно.

Это делает обработку чуть сложнее.

Из этого следует вывод, что `батчинг` — это способ повысить скорость и уменьшить нагрузку на сеть и брокер, но он вносит
дополнительные задержки и усложняет обработку ошибок. Применяют его там, где сообщений очень много (например, 
логирование, телеметрия, аналитика), но не используют в критичных операциях, где важна скорость и точность (например, платежи).

```java
@Configuration
public class RabbitBatchConfig {

    @Bean
    public BatchingRabbitTemplate batchingRabbitTemplate(ConnectionFactory connectionFactory) {
        // Стратегия пакетирования: максимальное количество сообщений, максимальный размер пакета (байты), таймаут ожидания
        SimpleBatchingStrategy batchingStrategy = new SimpleBatchingStrategy(
                100,       // макс. сообщений в пакете
                16384,     // макс. байт в пакете
                5000       // таймаут ожидания в мс
        );

        // Планировщик задач нужен для таймера
        ConcurrentTaskScheduler taskScheduler = new ConcurrentTaskScheduler();

        // Создаём batching RabbitTemplate
        BatchingRabbitTemplate template = new BatchingRabbitTemplate(connectionFactory, batchingStrategy, taskScheduler);
        template.setExchange("my.exchange"); // можно указать exchange по умолчанию
        return template;
    }
}
```
---

### Вопросы
- Что такое `Spring AMQP` и для чего он нужен?
- Что такое `RabbitTemplate` и для чего он используется?
- Какие решения от сбоев есть в `Spring AMQP`?
- Как в `Spring AMQP` реализован механизм `Retry`?
- Что такое `Publisher Confirms` и зачем они нужны? 
- Что такое `Returns Callback` и когда он вызывается? 
- Для чего нужен флаг `mandatory=true` при отправке сообщений?
- Зачем нужен `batching` при работе с `RabbitMQ`?
- Какие есть виды `batching`?

---

### Упражнения

1. Подключить зависимости `Spring Boot` и `Spring AMQP`.
2. Добавить `application.yml` с настройками `RabbitMQ`.
3. Расширение `Config`. В существующем `RabbitConfig` добавить:
   - Настройку `RabbitTemplate` с `Publisher Confirms` и `Returns Callback`.
   - Настройку `BatchingRabbitTemplate` для отправки сообщений пачками.
   - Настройку `concurrency` и `DLQ` (Dead Letter Queue).
4. Переписываем `Producer`. В `OrderService`:
   - Используем `RabbitTemplate` вместо ручного `AMQP`. 
   - Добавляем логирование времени отправки (`sentAt`).
5. Переписываем `Consumer`. В `NotificationService`:
   - Используем `@RabbitListener` вместо ручного контейнера. 
   - Вычисляем задержку доставки (`System.currentTimeMillis() - sentAt`). 
   - Включаем `retry` и `DLQ`.
6. Отправить несколько сообщений через `OrderService`. Проследить, как `NotificationService` их обрабатывает.
7. (Опционально) используем `batching` через `BatchingRabbitTemplate`.

---

## 📚 Полезные ссылки и материалы

### 📖 **Официальная документация**
- [RabbitMQ](https://www.rabbitmq.com/docs) - официальная документация RabbitMQ
- [RabbitMQ Tutorials](https://www.rabbitmq.com/tutorials/tutorial-one-java-stream) - официальные примеры
- [Spring AMQP](https://docs.spring.io/spring-amqp/reference/) - документация Spring AMQP

### 🔥 **Лучшие статьи с Хабра (2024-2025)**
- [Spring AMQP](https://habr.com/ru/companies/otus/articles/825528/) - обзор работы Spring AMQP
- [RabbitMQ: семантика и гарантия доставки сообщений](https://habr.com/ru/companies/itsumma/articles/437446/) - разбор надежности и подтверждениях в RabbitMQ
- [Лучшие практики для надёжной работы с RabbitMQ](https://habr.com/ru/companies/tochka/articles/799949/) - разбор стабильности обмена и сохранности данных

### 🛠 **Инструменты разработки**
- [Spring AMQP](https://spring.io/projects/spring-amqp) - интеграция `RabbitMQ` с Java/Spring
- [Testcontainers RabbitMQ](https://java.testcontainers.org/modules/rabbitmq/) – Инструмент для интеграционных тестов с RabbitMQ
- [RabbitMQ Delayed Message Plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange) – Плагин для отложенной доставки сообщений

### 🎯 **Практические примеры**
- [RabbitMQ Java Client Examples](https://github.com/rabbitmq/rabbitmq-tutorials/tree/main/spring-amqp) - реализация RabbitMQ Spring AMQP (GitHub)
- [RabbitMQ & Spring Boot](https://www.baeldung.com/spring-amqp) - реализация `Spring Boot` + `RabbitMQ`