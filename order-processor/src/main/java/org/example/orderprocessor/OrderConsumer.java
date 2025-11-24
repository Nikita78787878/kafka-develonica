package org.example.orderprocessor;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    @KafkaListener(topics = "orders.new", groupId = "order-service")
    public void handleOrder(String message){
        System.out.println("📥 Received: " + message);

    }

}
