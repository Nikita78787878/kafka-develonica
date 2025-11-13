package org.example.orderprocessor;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


/**
 * Для семантики менее одно раза At most once:
 *    В Консюмере:
 *      - оставляем авто коммит
 *    В продюсере:
 *          - ask: 0, т.к. по умолчанию 1
 */
public class OrderProcessorService {

    public static void main(String[] args) {
        new OrderProcessorService().processOrders();
    }

    public void processOrders() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-processor-group");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // ручной коммит

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             KafkaProducer<String, String> producer = new KafkaProducer<>(createProducerProps())) {

            consumer.subscribe(Collections.singletonList("orders.new"));
            System.out.println("👂 Консьюмер слушает топик orders.new ...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    long receiveTime = System.currentTimeMillis();
                    String orderId = record.key();
                    String orderData = record.value();

                    try {
                        // Проверка на poison pill
                        if (orderData == null || orderData.contains("poison")) {
                            System.err.printf("💀 Обнаружено Poison Pill [%s]: %s%n", orderId, orderData);
                            producer.send(new ProducerRecord<>("orders.reliable", orderId, orderData));
                            continue;
                        }

                        boolean valid = orderData.contains("@");
                        String targetTopic = valid ? "orders.processed" : "orders.failed";

                        long sendTime = extractTimestamp(orderData);
                        long latency = (sendTime > 0) ? receiveTime - sendTime : -1;

                        producer.send(new ProducerRecord<>(targetTopic, orderId, orderData));

                        System.out.printf("✅ Обработано [%s], топик → %s, задержка=%d мс%n",
                                orderId, targetTopic, latency);

                    } catch (Exception e) {
                        System.err.printf("❌ Ошибка обработки [%s]: %s%n", orderId, e.getMessage());
                        producer.send(new ProducerRecord<>("orders.reliable", orderId, orderData));
                    }
                }
            }
        }
    }

    private long extractTimestamp(String json) {
        try {
            int idx = json.indexOf("\"ts\":");
            if (idx == -1) return -1;
            String sub = json.substring(idx + 5).replaceAll("[^0-9]", "");
            return Long.parseLong(sub);
        } catch (Exception e) {
            return -1;
        }
    }

    private Properties createProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "0");
//        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return props;
    }
}
