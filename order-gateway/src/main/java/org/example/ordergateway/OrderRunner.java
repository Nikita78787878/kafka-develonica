package org.example.ordergateway;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
// @RequiredArgsConstructor <--- Тоже убираем, раз он не работает
public class OrderRunner implements CommandLineRunner {

    private final OrderProducerServiceAvro producerService;

    // --- КОНСТРУКТОР ВРУЧНУЮ ---
    public OrderRunner(OrderProducerServiceAvro producerService) {
        this.producerService = producerService;
    }
    // ---------------------------

    @Override
    public void run(String... args) throws Exception {
        System.out.println("🚀 Запуск Avro продюсера...");

        // Бесконечный цикл отправки каждые 3 секунды
        while (true) {
            try {
                producerService.sendOrder();
                Thread.sleep(3000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}