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
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

                for (ConsumerRecord<String, String> record : records) {
                    String orderId = record.key();
                    String orderData = record.value();

                    boolean valid = orderData.contains("@"); // простая валидация email
                    String targetTopic = valid ? "orders.processed" : "orders.failed";

                    producer.send(new ProducerRecord<>(targetTopic, orderId, orderData));

                    System.out.println(String.format("Консюмер принял сообщение c id: %s и значением amount: %s читая топик orders.new и отправил запись дальше в топик: %s. " +
                                    "Т.к. сообщение пришедшие в orders.new: %s",
                            orderId, orderData, targetTopic,  valid ? "✅ валидное" : "❌ не валидное"));

//                    consumer.commitSync(); // ручной коммит
                }
            }
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
