package org.example.ordergateway;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 📘 Семантика доставки сообщений в Kafka
 *
 *  Для семантики "менее одного раза" (At most once):
 *  Причина: при падении брокера не ждём ответа от него и пуляем дальше.
 *
 *    В консьюмере:
 *      - оставляем авто-коммит (enable.auto.commit = true)
 *
 *    В продюсере:
 *      - acks = 0      // по умолчанию 1
 *      - retries = 0
 *
 *  Для семантики "более одного раза" (At least once):
 *  Причина: при падении у нас «грязная» транзакция — уже записанные сообщения
 *           не откатываются, но оффсет может быть не зафиксирован.
 *
 *    В консьюмере:
 *      - ручной коммит (enable.auto.commit = false)
 *
 *    В продюсере:
 *      - acks = -1     // ждём подтверждение от всех реплик
 *      - retries = 3
 *
 *  Для семантики "ровно один раз" (Exactly once):
 *  Тут всё чётко — есть транзакции и идемпотентность (чтобы не было дубликатов).
 *
 *    В консьюмере:
 *      - всё как в "at least once"
 *      - уровень изоляции транзакций:
 *          config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
 *
 *    В продюсере:
 *      - всё как в "at least once"
 *      - включаем идемпотентность:
 *          config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
 *      - включаем транзакции:
 *          config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-001");
 *
 */

public class OrderGatewayService implements AutoCloseable {

    // Продюсер теперь - поле класса. Он будет жить, пока живет сервис.
    private final KafkaProducer<String, String> producer;
    private final String topic = "orders.new";

    /**
     * Конструктор. Здесь мы ОДИН РАЗ создаем продюсера.
     */
    public OrderGatewayService() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.RETRIES_CONFIG, "0");

        //Пришлось добавлять чтобы увидеть сбой
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5000"); // 5 секунд буфер
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768"); // 32 KB

        // props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-tx-1");

        // Создаем экземпляр продюсера
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Главный метод. Теперь он использует try-with-resources.
     */
    public static void main(String[] args) throws InterruptedException {

        // Используем try-with-resources. Когда main завершится (Ctrl+C),
        // у service автоматически вызовется метод close()
        try (OrderGatewayService service = new OrderGatewayService()) {

            int i = 200;
            System.out.println("🚀 Продюсер запущен. Отправка сообщений... (Ctrl+C для выхода)");

            while (true) {

                // --- ИСПРАВЛЕННАЯ ЛОГИКА ---
                // 1. Готовим данные (JSON-строку) с нужным amount
                String data1 = String.format("{ \"email\": \"user@example.com\", \"amount\": %d }", i);
                String data2 = String.format("{ \"email\": \"bad-email\", \"amount\": %d }", i);

                // 2. Отправляем, используя СТАТИЧНЫЕ ключи и готовые данные
                service.sendOrder("order-1", data1);
                service.sendOrder("order-2", data2);

                Thread.sleep(1000);
                i++;
            }
        }
    }

    /**
     * Метод отправки. Теперь он НЕ СОЗДАЕТ продюсера, а использует
     * тот, что был создан в конструкторе (this.producer).
     */
    public void sendOrder(String orderId, String orderData) {

        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, orderId, orderData);

        // Просто используем `producer`, который уже есть
        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
            if (exception == null) {
                // При acks=0 metadata почти бесполезна (offset будет -1), т.к. мы не ждем ответа
                System.out.printf("✅ (Предположительно) Записалось: ключ: %s, значение: %s\n",
                        orderId, orderData);
            } else {
                // А вот сюда будет прилетать ошибка, когда ты остановишь 'docker stop kafka'
                System.err.printf("❌ Вернулась ошибка, ключ: %s; ошибка: %s\n",
                        orderId, exception.getMessage());
            }
        });
    }

    /**
     * Этот метод вызовется автоматически благодаря try-with-resources в main.
     * Он нужен для корректного закрытия продюсера.
     */
    @Override
    public void close() {
        System.out.println("Закрываю продюсер...");
        producer.flush(); // Сначала "сбрасываем" все, что есть в буфере
        producer.close(); // Теперь корректно закрываем
    }
}

