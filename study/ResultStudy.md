По уроку 4 не получилось воспроизвести потерю данных, данные собирались в пакет и пакетом отправлялись.
После падения докера ссыпались ошибки по типу

Консьюмер
13:51:48.228 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:48.228 [main] INFO org.apache.kafka.clients.NetworkClient -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Node -1 disconnected.
13:51:48.231 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node -1 disconnected.
13:51:48.232 [main] INFO org.apache.kafka.clients.NetworkClient -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Node 1 disconnected.
13:51:48.232 [main] INFO org.apache.kafka.clients.NetworkClient -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Cancelled in-flight FETCH request with correlation id 288 due to node 1 being disconnected (elapsed time since creation: 10ms, elapsed time since send: 9ms, throttle time: 0ms, request timeout: 30000ms)
13:51:48.232 [main] INFO org.apache.kafka.clients.NetworkClient -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Cancelled in-flight METADATA request with correlation id 289 due to node 1 being disconnected (elapsed time since creation: 9ms, elapsed time since send: 9ms, throttle time: 0ms, request timeout: 30000ms)
13:51:48.233 [main] INFO org.apache.kafka.clients.NetworkClient -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Node 2147483646 disconnected.
13:51:48.233 [main] INFO org.apache.kafka.clients.FetchSessionHandler -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Error sending fetch request (sessionId=726496658, epoch=225) to node 1:
org.apache.kafka.common.errors.DisconnectException: null
13:51:48.236 [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Group coordinator localhost:9092 (id: 2147483646 rack: null) is unavailable or invalid due to cause: coordinator unavailable. isDisconnected: true. Rediscovery will be attempted.
13:51:48.346 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:48.346 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Cancelled in-flight API_VERSIONS request with correlation id 28 due to node 1 being disconnected (elapsed time since creation: 2ms, elapsed time since send: 2ms, throttle time: 0ms, request timeout: 30000ms)
13:51:48.392 [main] INFO org.apache.kafka.clients.NetworkClient -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Node 1 disconnected.
13:51:48.392 [main] INFO org.apache.kafka.clients.NetworkClient -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Cancelled in-flight API_VERSIONS request with correlation id 290 due to node 1 being disconnected (elapsed time since creation: 1ms, elapsed time since send: 1ms, throttle time: 0ms, request timeout: 30000ms)
13:51:48.520 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:48.520 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Cancelled in-flight API_VERSIONS request with correlation id 29 due to node 1 being disconnected (elapsed time since creation: 1ms, elapsed time since send: 1ms, throttle time: 0ms, request timeout: 30000ms)
13:51:48.562 [main] INFO org.apache.kafka.clients.NetworkClient -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Node 1 disconnected.
13:51:48.562 [main] INFO org.apache.kafka.clients.NetworkClient -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Cancelled in-flight API_VERSIONS request with correlation id 291 due to node 1 being disconnected (elapsed time since creation: 1ms, elapsed time since send: 1ms, throttle time: 0ms, request timeout: 30000ms)
13:51:48.867 [main] INFO org.apache.kafka.clients.NetworkClient -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Node 1 disconnected.
13:51:48.867 [main] INFO org.apache.kafka.clients.NetworkClient -- [Consumer clientId=consumer-order-processor-group-1, groupId=order-processor-group] Cancelled in-flight API_VERSIONS request with correlation id 292 due to node 1 being disconnected (elapsed time since creation: 3ms, elapsed time since send: 3ms, throttle time: 0ms, request timeout: 30000ms)
13:51:48.998 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.

Продьюсер
13:51:48.228 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:48.231 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node -1 disconnected.
13:51:48.351 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:48.352 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Cancelled in-flight API_VERSIONS request with correlation id 13 due to node 1 being disconnected (elapsed time since creation: 0ms, elapsed time since send: 0ms, throttle time: 0ms, request timeout: 30000ms)
13:51:48.567 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:48.567 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Cancelled in-flight API_VERSIONS request with correlation id 14 due to node 1 being disconnected (elapsed time since creation: 1ms, elapsed time since send: 1ms, throttle time: 0ms, request timeout: 30000ms)
13:51:48.957 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:48.957 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Cancelled in-flight API_VERSIONS request with correlation id 15 due to node 1 being disconnected (elapsed time since creation: 2ms, elapsed time since send: 2ms, throttle time: 0ms, request timeout: 30000ms)
13:51:49.374 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:49.374 [kafka-producer-network-thread | producer-1] WARN org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Connection to node 1 (localhost/127.0.0.1:9092) could not be established. Node may not be available.
13:51:50.210 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:50.210 [kafka-producer-network-thread | producer-1] WARN org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Connection to node 1 (localhost/127.0.0.1:9092) could not be established. Node may not be available.
13:51:51.221 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:51.222 [kafka-producer-network-thread | producer-1] WARN org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Connection to node 1 (localhost/127.0.0.1:9092) could not be established. Node may not be available.
13:51:52.229 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:52.229 [kafka-producer-network-thread | producer-1] WARN org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Connection to node 1 (localhost/127.0.0.1:9092) could not be established. Node may not be available.
13:51:53.238 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:53.238 [kafka-producer-network-thread | producer-1] WARN org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Connection to node 1 (localhost/127.0.0.1:9092) could not be established. Node may not be available.
13:51:54.106 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.
13:51:54.107 [kafka-producer-network-thread | producer-1] WARN org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Connection to node 1 (localhost/127.0.0.1:9092) could not be established. Node may not be available.
13:51:55.112 [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.NetworkClient -- [Producer clientId=producer-1] Node 1 disconnected.

Объяснение ИИ
⚙️ Что произошло


Консьюмер
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 230 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 231 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 232 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 233 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 234 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 230 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 231 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 232 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 233 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 234 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное

Уронили докер на 10 сек
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 235 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 236 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 237 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 238 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 239 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 240 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 241 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 242 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 243 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 244 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 245 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 246 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 247 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 248 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 249 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 250 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 251 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 252 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 253 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 254 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 255 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 256 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 257 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 258 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 259 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 260 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 261 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 262 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 263 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 235 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 236 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 237 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 238 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 239 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 240 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 241 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 242 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 243 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 244 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 245 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 246 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 247 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 248 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 249 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 250 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 251 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 252 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 253 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 254 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 255 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 256 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 257 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 258 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 259 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 260 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 261 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 262 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 263 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 264 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 265 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 266 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 267 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-2 и значением amount: { "email": "bad-email", "amount": 268 } читая топик orders.new и отправил запись дальше в топик: orders.failed. Т.к. сообщение пришедшие в orders.new: ❌ не валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 264 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 265 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 266 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 267 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-1 и значением amount: { "email": "user@example.com", "amount": 268 } читая топик orders.new и отправил запись дальше в топик: orders.processed. Т.к. сообщение пришедшие в orders.new: ✅ валидное
Консюмер принял сообщение c id: order-2 и значением amount: {

В продюсере:

acks=0 → продюсер не ждёт подтверждения от брокера, но Kafka клиент всё равно кладёт сообщение в send buffer, и в фоне NetworkClient отправляет пакеты на сокет.

linger.ms=5000 и batch.size=32768 → сообщения собираются в батчи. Но как только истекает linger или размер превышен — батч уходит в сеть.

Если Kafka-брокер доступен, то эти данные в момент сброса батча уже попадают в log segment на диске брокера (пусть даже не подтверждаются).

Вы остановили Docker → сокет закрывается, но все сообщения, которые уже были в буфере сети/системы до момента отключения, всё равно доходят до брокера или записаны на диск.
Поэтому потерю вы не увидели.

После перезапуска брокер автоматически восстановил соединение, и консьюмер продолжил читать дальше.

