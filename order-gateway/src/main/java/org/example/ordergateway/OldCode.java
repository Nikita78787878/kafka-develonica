//package org.example.ordergateway;
//
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.apache.kafka.common.serialization.StringSerializer;
//
//import java.util.Properties;
//
//
//
///**
// * Для семантики менее одно раза At most once:
// *    В Консюмере:
// *      - оставляем авто коммит
// *    В продюсере:
// *          - ask: 0, т.к. по умолчанию 1
// */
//
//
//public class OrderGatewayService {
//
//    public static void main(String[] args) throws InterruptedException {
//        OrderGatewayService service = new OrderGatewayService();
//
//        int i = 55;
//        while(true){
//            service.sendOrder(String.format("order-1", "{ \"email\": \"user@example.com\", \"amount\": %d }"), String.valueOf(i));
//            service.sendOrder(String.format("order-2", "{ \"email\": \"bad-email\", \"amount\": %d }"), String.valueOf(i));
//            Thread.sleep(2000);
//            i++;
//        }
//
//
//    }
//
//    public void sendOrder(String orderId, String orderData) {
//        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.ACKS_CONFIG, "0");
////        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // идемпотентность
////        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-tx-1"); // Транзакционность
//
//        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
//
//            ProducerRecord<String, String> record =
//                    new ProducerRecord<>("orders.new", orderId, orderData);
//
//            producer.send(record, (RecordMetadata metadata, Exception exception) -> {
//                if (exception == null) {
//                    System.out.printf("✅ Записалось: ключ: %s; топик: %s; партиция: %s cмещение: %s",
//                            orderId, metadata.topic(), metadata.partition(), metadata.offset());
//                } else {
//                    System.err.printf("❌ Вернулась ошибка, ключ: %s; ошибка: %s",
//                            orderId, exception.getMessage());
//                }
//            });
//        }
//    }
//}
