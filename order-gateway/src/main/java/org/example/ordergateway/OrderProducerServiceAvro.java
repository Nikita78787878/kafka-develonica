package org.example.ordergateway;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.example.event.OrderPlaced;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
// @RequiredArgsConstructor <--- УБИРАЕМ ЭТО, ОНО ГЛЮЧИТ У ТЕБЯ
public class OrderProducerServiceAvro {

    private final KafkaTemplate<String, OrderPlaced> kafkaTemplate;

    // --- ДОБАВЛЯЕМ КОНСТРУКТОР ВРУЧНУЮ ---
    // Spring видит этот конструктор и сам подставляет (инжектит) сюда готовый KafkaTemplate
    public OrderProducerServiceAvro(KafkaTemplate<String, OrderPlaced> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    // -------------------------------------

    public void sendOrder() {
        String orderId = UUID.randomUUID().toString();

        OrderPlaced event = OrderPlaced.newBuilder()
                .setOrderId(orderId)
                .setCustomerId("user-555")
                .setItems(List.of("iphone", "macbook"))
                .setTotalAmount(1999.99)
                .setTimestamp(Instant.now().toEpochMilli())
                .build();

        CompletableFuture<SendResult<String, OrderPlaced>> future =
                kafkaTemplate.send("orders.avro", event.getOrderId().toString(), event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("✅ [Avro] Отправлено: " + event.getOrderId());
            } else {
                System.err.println("❌ Ошибка отправки: " + ex.getMessage());
            }
        });
    }
}