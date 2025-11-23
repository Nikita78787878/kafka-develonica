package org.example.orderprocessor;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.example.event.OrderPlaced; // Тот же самый класс
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderProcessorServiceAvro {

    // Аннотация делает всю магию: подписывается, поллит, коммитит
    @KafkaListener(topics = "orders.avro", groupId = "order-processor-group")
    public void listen(ConsumerRecord<String, OrderPlaced> record) {

        // Достаем ключ и значение
        String key = record.key();
        OrderPlaced event = record.value();

        System.out.printf("📥 [Avro] Получено сообщение!%n");
        System.out.printf("   Key: %s%n", key);
        System.out.printf("   Customer: %s%n", event.getCustomerId());
        System.out.printf("   Total: %s%n", event.getTotalAmount());
        System.out.printf("   Items: %s%n", event.getItems());
        System.out.println("---------------------------------");
    }
}
