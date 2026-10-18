package org.vivek.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trade.orderservice.dag.DAGExecutor;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.vivek.commonmodule.model.CancellationEvent;
import org.vivek.commonmodule.model.Order;
import org.vivek.commonmodule.model.OrderSide;
import org.vivek.commonmodule.model.OrderStatus;
import org.vivek.commonmodule.model.OrderType;
import org.vivek.commonmodule.model.TradeExecution;
import org.vivek.ledgerservice.entity.LedgerEntryEntity;
import org.vivek.ledgerservice.entity.ProcessedEvent;
import org.vivek.ledgerservice.entity.UserBalance;
import org.vivek.ledgerservice.entity.UserHolding;
import org.vivek.ledgerservice.entity.UserHoldingId;
import org.vivek.ledgerservice.LedgerConsumer;
import org.vivek.ledgerservice.repository.LedgerEntryRepository;
import org.vivek.ledgerservice.repository.ProcessedEventRepository;
import org.vivek.ledgerservice.repository.UserBalanceRepository;
import org.vivek.ledgerservice.repository.UserHoldingRepository;
import org.vivek.matchingengine.orderbook.SymbolOrderBook;
import org.vivek.order.client.MatchingEngineClient;
import org.vivek.order.client.MatchingEngineResponse;
import org.vivek.order.repository.OrderRepository;
import org.vivek.order.service.OrderService;
import org.vivek.trade.compliance.grpc.ComplianceServiceGrpc;
import org.vivek.trade.compliance.grpc.ValidationRequest;
import org.vivek.trade.compliance.grpc.ValidationResponse;
import org.vivek.trade.margin.grpc.MarginServiceGrpc;
import org.vivek.trade.risk.grpc.RiskServiceGrpc;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.util.backoff.FixedBackOff;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Testcontainers
@SpringBootTest(classes = OrderFlowIntegrationTest.IntegrationTestConfig.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestPropertySource(properties = {
        "spring.main.allow-bean-definition-overriding=true",
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=ledger-it-group",
        "kafka.consumer.topic=trade-executed",
        "kafka.consumer.cancellation-topic=order-cancelled",
        "resilience4j.circuitbreaker.instances.riskService.minimumNumberOfCalls=15",
        "resilience4j.circuitbreaker.instances.riskService.slidingWindowSize=20",
        "resilience4j.circuitbreaker.instances.riskService.failureRateThreshold=50",
        "resilience4j.circuitbreaker.instances.riskService.waitDurationInOpenState=30s",
        "resilience4j.circuitbreaker.instances.riskService.permittedCallsInHalfOpenState=3"
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OrderFlowIntegrationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0")
    );

    private static final GrpcServerRule RISK_GRPC = new GrpcServerRule().directExecutor();
    private static final GrpcServerRule MARGIN_GRPC = new GrpcServerRule().directExecutor();
    private static final GrpcServerRule COMPLIANCE_GRPC = new GrpcServerRule().directExecutor();

    private static final RiskMockService RISK_MOCK = new RiskMockService();
    private static final MarginMockService MARGIN_MOCK = new MarginMockService();
    private static final ComplianceMockService COMPLIANCE_MOCK = new ComplianceMockService();

    private static volatile boolean grpcStarted;

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        if (!KAFKA.isRunning()) {
            KAFKA.start();
        }
        ensureGrpcStarted();

        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("KAFKA_BOOTSTRAP_SERVERS", KAFKA::getBootstrapServers);
    }

    @Autowired
    private TrackingOrderRepository orderRepository;

    @Autowired
    private OrderService orderService;

    @Autowired
    private SymbolOrderBook orderBook;

    @Autowired
    private InMemoryLedger inMemoryLedger;

    @Autowired
    private LedgerConsumer ledgerConsumer;

    @Autowired
    private InProcessMatchingEngineClient matchingEngineClient;

    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    @BeforeEach
    void resetState() {
        orderRepository.clearAll();
        resetOrderBook(orderBook);
        inMemoryLedger.reset();

        matchingEngineClient.resetRoutingGate();

        RISK_MOCK.reset();
        MARGIN_MOCK.reset();
        COMPLIANCE_MOCK.reset();

        circuitBreakerRegistry.circuitBreaker("riskService").reset();
    }

    @AfterAll
    void shutdownResources() {
        stopGrpcRule(RISK_GRPC);
        stopGrpcRule(MARGIN_GRPC);
        stopGrpcRule(COMPLIANCE_GRPC);
        if (KAFKA.isRunning()) {
            KAFKA.stop();
        }
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Proves happy path: VALIDATING -> APPROVED -> EXECUTED with Kafka + ledger updates")
    void happy_path_order_fully_matched() {
        String orderId = "BUY-HP-" + UUID.randomUUID();
        String sellerOrderId = "SELL-HP-" + UUID.randomUUID();

        Order buy = newOrder(orderId, "buyer-hp", OrderSide.BUY, 100.0d, 100.0d, OrderType.LIMIT);

        matchingEngineClient.blockRoutingUntilReleased();
        orderService.processOrder(buy);

        Awaitility.await().atMost(2, TimeUnit.SECONDS)
                .until(() -> orderRepository.hasStatus(orderId, OrderStatus.VALIDATING));
        Awaitility.await().atMost(2, TimeUnit.SECONDS)
                .until(() -> orderRepository.hasStatus(orderId, OrderStatus.APPROVED));

        assertEquals(1, RISK_MOCK.callCount(), "Risk gRPC should be called exactly once");
        assertEquals(1, MARGIN_MOCK.callCount(), "Margin gRPC should be called exactly once");
        assertEquals(1, COMPLIANCE_MOCK.callCount(), "Compliance gRPC should be called exactly once");

        Order sell = newOrder(sellerOrderId, "seller-hp", OrderSide.SELL, 100.0d, 100.0d, OrderType.LIMIT);
        orderBook.match(sell);

        matchingEngineClient.releaseRouting();

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> statusOf(orderId) == OrderStatus.EXECUTED);

        List<ConsumerRecord<String, byte[]>> tradeMessages = pollTopic("trade-executed", Duration.ofSeconds(3));
        long matchingOrderMessages = tradeMessages.stream()
                .filter(record -> orderId.equals(record.key()))
                .count();
        assertEquals(1L, matchingOrderMessages, "trade-executed should contain exactly one message for this order");

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> ((Number) ledgerConsumer.getBalance("buyer-hp").get("balance")).doubleValue() < 0.0d);

        printPassed("happy path order is validated, approved, executed, and reflected in Kafka + ledger");
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("Proves order is rejected when risk fails while margin/compliance still execute")
    void order_rejected_when_risk_fails() {
        String orderId = "BUY-RISK-" + UUID.randomUUID();
        Order order = newOrder(orderId, "buyer-risk", OrderSide.BUY, 10.0d, 99.0d, OrderType.LIMIT);

        RISK_MOCK.setMode(MockMode.FAIL_RESPONSE);

        orderService.processOrder(order);

        Awaitility.await().atMost(2, TimeUnit.SECONDS)
                .until(() -> statusOf(orderId) == OrderStatus.REJECTED);

        assertTrue(MARGIN_MOCK.callCount() >= 1, "Margin gRPC should still be called in parallel");
        assertTrue(COMPLIANCE_MOCK.callCount() >= 1, "Compliance gRPC should still be called in parallel");

        List<ConsumerRecord<String, byte[]>> tradeMessages = pollTopic("trade-executed", Duration.ofSeconds(2));
        boolean hasMessageForOrder = tradeMessages.stream().anyMatch(record -> orderId.equals(record.key()));
        assertFalse(hasMessageForOrder, "No trade-executed message should be published for rejected order");

        printPassed("risk failure rejects the order while other parallel validators still run");
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Proves IOC partial fill publishes trade, cancels remainder, and leaves no resting buy order")
    void ioc_order_partially_matched_remainder_cancelled() {
        String sellOrderId = "SELL-IOC-" + UUID.randomUUID();
        String buyOrderId = "BUY-IOC-" + UUID.randomUUID();

        Order restingSell = newOrder(sellOrderId, "seller-ioc", OrderSide.SELL, 40.0d, 100.0d, OrderType.LIMIT);
        orderBook.match(restingSell);

        Order iocBuy = newOrder(buyOrderId, "buyer-ioc", OrderSide.BUY, 100.0d, 100.0d, OrderType.IOC);
        orderService.processOrder(iocBuy);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> orderRepository.hasStatus(buyOrderId, OrderStatus.PARTIALLY_FILLED));
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> statusOf(buyOrderId) == OrderStatus.CANCELLED);

        List<ConsumerRecord<String, byte[]>> tradeMessages = pollTopic("trade-executed", Duration.ofSeconds(3));
        boolean hasQty40 = tradeMessages.stream()
                .filter(record -> buyOrderId.equals(record.key()))
                .map(record -> readJson(record.value()))
                .anyMatch(payload -> Math.abs(payload.path("quantity").asDouble() - 40.0d) < 0.000001d);
        assertTrue(hasQty40, "TradeExecution with quantity=40 must be published");

        assertFalse(hasBuyOrderInBook(orderBook, buyOrderId), "IOC remainder must not rest on the buy book");

        printPassed("IOC partial fill publishes only matched quantity and cancels the unfilled remainder");
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Proves circuit breaker opens and sixth order fails fast after repeated risk failures")
    void circuit_breaker_trips_after_failures() {
        RISK_MOCK.setMode(MockMode.THROW_EXCEPTION);
        RISK_MOCK.setThrowCount(50);

        List<Long> durationsMs = new ArrayList<>();

        for (int i = 0; i < 6; i++) {
            String orderId = "BUY-CB-" + i + "-" + UUID.randomUUID();
            Order order = newOrder(orderId, "buyer-cb", OrderSide.BUY, 1.0d, 100.0d, OrderType.LIMIT);

            long startNs = System.nanoTime();
            orderService.processOrder(order);

            Awaitility.await().atMost(12, TimeUnit.SECONDS)
                    .until(() -> {
                        OrderStatus status = statusOf(orderId);
                        return status == OrderStatus.REJECTED || status == OrderStatus.FAILED;
                    });
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            durationsMs.add(elapsedMs);
        }

        for (int i = 0; i < 5; i++) {
            assertTrue(durationsMs.get(i) >= 150L, "First five orders should go through failure path, got " + durationsMs.get(i) + " ms");
        }
        assertTrue(durationsMs.get(5) < 150L, "Sixth order should fail fast when breaker is open, got " + durationsMs.get(5) + " ms");

        CircuitBreaker.State state = circuitBreakerRegistry.circuitBreaker("riskService").getState();
        assertEquals(CircuitBreaker.State.OPEN, state, "riskService circuit breaker should be OPEN");

        printPassed("risk circuit breaker opens after repeated failures and fast-fails subsequent orders");
    }

    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("Proves malformed trade events are retried and moved to trade-executed.DLT with exception headers")
    void dlq_receives_failed_ledger_event() {
        String malformedKey = "MALFORMED-" + UUID.randomUUID();
        publishMalformedTradeEvent(malformedKey);

        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .until(() -> pollTopic("trade-executed.DLT", Duration.ofSeconds(2)).stream()
                        .anyMatch(record -> malformedKey.equals(record.key())));

        ConsumerRecord<String, byte[]> dltRecord = pollTopic("trade-executed.DLT", Duration.ofSeconds(2)).stream()
                .filter(record -> malformedKey.equals(record.key()))
                .findFirst()
                .orElseThrow();

        assertTrue(dltRecord.headers().lastHeader("X-Exception-Message") != null,
                "DLT message must include X-Exception-Message header");

        printPassed("malformed ledger events are retried and then routed to DLT with exception metadata");
    }

    private void publishMalformedTradeEvent(String key) {
        Map<String, Object> producerProps = new java.util.HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>("trade-executed", key, "THIS-IS-NOT-VALID-JSON"));
            producer.flush();
        }
    }

    private List<ConsumerRecord<String, byte[]>> pollTopic(String topic, Duration timeout) {
        Map<String, Object> consumerProps = new java.util.HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "it-consumer-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach(records::add);
            }
        }

        return records;
    }

    private OrderStatus statusOf(String orderId) {
        Order order = orderRepository.findById(orderId);
        return order != null ? order.getStatus() : null;
    }

    private Order newOrder(String orderId,
                           String userId,
                           OrderSide side,
                           double quantity,
                           double price,
                           OrderType orderType) {
        return Order.builder()
                .orderId(orderId)
                .userId(userId)
                .symbol("INFY")
                .side(side)
                .quantity(quantity)
                .price(price)
                .orderType(orderType)
                .status(OrderStatus.PENDING)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
    }

    private static void resetOrderBook(SymbolOrderBook orderBook) {
        clearMapField(orderBook, "buyOrders");
        clearMapField(orderBook, "sellOrders");
    }

    private static boolean hasBuyOrderInBook(SymbolOrderBook orderBook, String orderId) {
        Object buyOrders = readField(orderBook, "buyOrders");
        if (!(buyOrders instanceof Map<?, ?> levels)) {
            return false;
        }

        for (Object queueObj : levels.values()) {
            if (!(queueObj instanceof Iterable<?> iterable)) {
                continue;
            }
            for (Object orderObj : iterable) {
                if (orderObj instanceof Order order && orderId.equals(order.getOrderId())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void clearMapField(Object target, String fieldName) {
        Object value = readField(target, fieldName);
        if (value instanceof Map<?, ?> map) {
            map.clear();
        }
    }

    private static Object readField(Object target, String fieldName) {
        try {
            Field field = findField(target.getClass(), fieldName);
            field.setAccessible(true);
            return field.get(target);
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to access field " + fieldName, exception);
        }
    }

    private static Field findField(Class<?> type, String fieldName) throws NoSuchFieldException {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private static JsonNode readJson(byte[] payload) {
        try {
            return OBJECT_MAPPER.readTree(payload);
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to parse JSON payload", exception);
        }
    }

    private static void printPassed(String whatWasProved) {
        System.out.println("PASSED: " + whatWasProved);
    }

    private static synchronized void ensureGrpcStarted() {
        if (grpcStarted) {
            return;
        }

        startGrpcRule(RISK_GRPC);
        startGrpcRule(MARGIN_GRPC);
        startGrpcRule(COMPLIANCE_GRPC);

        RISK_GRPC.getServiceRegistry().addService(RISK_MOCK);
        MARGIN_GRPC.getServiceRegistry().addService(MARGIN_MOCK);
        COMPLIANCE_GRPC.getServiceRegistry().addService(COMPLIANCE_MOCK);

        grpcStarted = true;
    }

    private static void startGrpcRule(GrpcServerRule rule) {
        invokeGrpcLifecycle(rule, "before");
    }

    private static void stopGrpcRule(GrpcServerRule rule) {
        invokeGrpcLifecycle(rule, "after");
    }

    private static void invokeGrpcLifecycle(GrpcServerRule rule, String methodName) {
        try {
            Class<?> current = rule.getClass();
            while (current != null) {
                try {
                    java.lang.reflect.Method method = current.getDeclaredMethod(methodName);
                    method.setAccessible(true);
                    method.invoke(rule);
                    return;
                } catch (NoSuchMethodException ignored) {
                    current = current.getSuperclass();
                }
            }
            throw new NoSuchMethodException(methodName);
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to invoke gRPC lifecycle method: " + methodName, exception);
        }
    }

    enum MockMode {
        SUCCESS,
        FAIL_RESPONSE,
        THROW_EXCEPTION
    }

    static class RiskMockService extends RiskServiceGrpc.RiskServiceImplBase {
        private final AtomicInteger calls = new AtomicInteger(0);
        private volatile MockMode mode = MockMode.SUCCESS;
        private volatile int throwCount;

        @Override
        public void validate(org.vivek.trade.risk.grpc.ValidationRequest request,
                             StreamObserver<org.vivek.trade.risk.grpc.ValidationResponse> responseObserver) {
            calls.incrementAndGet();

            if (mode == MockMode.THROW_EXCEPTION && throwCount > 0) {
                throwCount--;
                responseObserver.onError(Status.INTERNAL.withDescription("risk failure").asRuntimeException());
                return;
            }

            boolean success = mode != MockMode.FAIL_RESPONSE;
            org.vivek.trade.risk.grpc.ValidationResponse response = org.vivek.trade.risk.grpc.ValidationResponse
                    .newBuilder()
                    .setSuccess(success)
                    .setServiceId("risk-service")
                    .setReason(success ? "OK" : "RISK_REJECTED")
                    .setLatencyMs(5)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        int callCount() {
            return calls.get();
        }

        void setMode(MockMode mode) {
            this.mode = mode;
        }

        void setThrowCount(int throwCount) {
            this.throwCount = throwCount;
        }

        void reset() {
            calls.set(0);
            mode = MockMode.SUCCESS;
            throwCount = 0;
        }
    }

    static class MarginMockService extends MarginServiceGrpc.MarginServiceImplBase {
        private final AtomicInteger calls = new AtomicInteger(0);
        private volatile MockMode mode = MockMode.SUCCESS;

        @Override
        public void validate(org.vivek.trade.margin.grpc.ValidationRequest request,
                             StreamObserver<org.vivek.trade.margin.grpc.ValidationResponse> responseObserver) {
            calls.incrementAndGet();

            boolean success = mode != MockMode.FAIL_RESPONSE;
            org.vivek.trade.margin.grpc.ValidationResponse response = org.vivek.trade.margin.grpc.ValidationResponse
                    .newBuilder()
                    .setSuccess(success)
                    .setServiceId("margin-service")
                    .setReason(success ? "OK" : "MARGIN_REJECTED")
                    .setLatencyMs(5)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        int callCount() {
            return calls.get();
        }

        void reset() {
            calls.set(0);
            mode = MockMode.SUCCESS;
        }
    }

    static class ComplianceMockService extends ComplianceServiceGrpc.ComplianceServiceImplBase {
        private final AtomicInteger calls = new AtomicInteger(0);
        private volatile MockMode mode = MockMode.SUCCESS;

        @Override
        public void validate(ValidationRequest request, StreamObserver<ValidationResponse> responseObserver) {
            calls.incrementAndGet();

            boolean success = mode != MockMode.FAIL_RESPONSE;
            ValidationResponse response = ValidationResponse
                    .newBuilder()
                    .setSuccess(success)
                    .setServiceId("compliance-service")
                    .setReason(success ? "OK" : "COMPLIANCE_REJECTED")
                    .setLatencyMs(5)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        int callCount() {
            return calls.get();
        }

        void reset() {
            calls.set(0);
            mode = MockMode.SUCCESS;
        }
    }

    static class TrackingOrderRepository extends OrderRepository {
        private final Map<String, List<OrderStatus>> statusHistory = new ConcurrentHashMap<>();

        @Override
        public synchronized Order save(Order order) {
            Order saved = super.save(order);
            if (order != null && order.getOrderId() != null && order.getStatus() != null) {
                statusHistory.computeIfAbsent(order.getOrderId(), ignored -> new CopyOnWriteArrayList<>())
                        .add(order.getStatus());
            }
            return saved;
        }

        boolean hasStatus(String orderId, OrderStatus status) {
            return statusHistory.getOrDefault(orderId, List.of()).contains(status);
        }

        void clearAll() {
            clearOrderStore();
            statusHistory.clear();
        }

        @SuppressWarnings("unchecked")
        private void clearOrderStore() {
            try {
                Field field = OrderRepository.class.getDeclaredField("orderStore");
                field.setAccessible(true);
                ((Map<String, Order>) field.get(this)).clear();
            } catch (Exception exception) {
                throw new IllegalStateException("Unable to clear order repository", exception);
            }
        }
    }

    static class InProcessMatchingEngineClient extends MatchingEngineClient {
        private final SymbolOrderBook orderBook;
        private final KafkaTemplate<String, TradeExecution> tradeKafkaTemplate;
        private final KafkaTemplate<String, CancellationEvent> cancellationKafkaTemplate;

        private volatile CountDownLatch routingGate = new CountDownLatch(0);

        InProcessMatchingEngineClient(SymbolOrderBook orderBook,
                                      KafkaTemplate<String, TradeExecution> tradeKafkaTemplate,
                                      KafkaTemplate<String, CancellationEvent> cancellationKafkaTemplate) {
            super("http://localhost:8081");
            this.orderBook = orderBook;
            this.tradeKafkaTemplate = tradeKafkaTemplate;
            this.cancellationKafkaTemplate = cancellationKafkaTemplate;
        }

        void blockRoutingUntilReleased() {
            routingGate = new CountDownLatch(1);
        }

        void releaseRouting() {
            routingGate.countDown();
        }

        void resetRoutingGate() {
            routingGate = new CountDownLatch(0);
        }

        @Override
        public MatchingEngineResponse route(Order order) {
            try {
                routingGate.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            }

            List<TradeExecution> executions = orderBook.match(order);
            double totalFilled = executions.stream().mapToDouble(TradeExecution::getQuantity).sum();
            double remainingQty = Math.max(0.0d, order.getQuantity() - totalFilled);

            for (TradeExecution tradeExecution : executions) {
                tradeKafkaTemplate.send(
                        org.vivek.matchingengine.config.KafkaProducerConfig.TOPIC_TRADE_EXECUTED,
                        order.getOrderId(),
                        tradeExecution
                );
            }

            MatchingEngineResponse response = new MatchingEngineResponse();
            response.setMatched(!executions.isEmpty());
            response.setFillCount(executions.size());
            response.setTotalFilled(totalFilled);
            response.setRemainingQty(remainingQty);
            return response;
        }

        @Override
        public boolean cancel(String orderId) {
            Order cancelled = orderBook.cancelOrder(orderId);
            if (cancelled == null) {
                return false;
            }

            CancellationEvent event = CancellationEvent.builder()
                    .orderId(cancelled.getOrderId())
                    .userId(cancelled.getUserId())
                    .symbol(cancelled.getSymbol())
                    .cancelledAt(Instant.now())
                    .build();

            cancellationKafkaTemplate.send(
                    org.vivek.matchingengine.config.KafkaProducerConfig.TOPIC_ORDER_CANCELLED,
                    cancelled.getOrderId(),
                    event
            );
            return true;
        }
    }

    static class InMemoryLedger {
        final Map<String, UserBalance> balances = new ConcurrentHashMap<>();
        final Map<UserHoldingId, UserHolding> holdings = new ConcurrentHashMap<>();
        final Set<String> processedEvents = ConcurrentHashMap.newKeySet();
        final UserBalanceRepository userBalanceRepository = mock(UserBalanceRepository.class);
        final UserHoldingRepository userHoldingRepository = mock(UserHoldingRepository.class);
        final LedgerEntryRepository ledgerEntryRepository = mock(LedgerEntryRepository.class);
        final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);

        InMemoryLedger() {
            when(userBalanceRepository.findById(anyString())).thenAnswer(invocation ->
                    Optional.ofNullable(balances.get(invocation.getArgument(0))));
            when(userBalanceRepository.save(any(UserBalance.class))).thenAnswer(invocation -> {
                UserBalance balance = invocation.getArgument(0);
                balances.put(balance.getUserId(), balance);
                return balance;
            });
            when(userHoldingRepository.findById(any(UserHoldingId.class))).thenAnswer(invocation ->
                    Optional.ofNullable(holdings.get(invocation.getArgument(0))));
            when(userHoldingRepository.save(any(UserHolding.class))).thenAnswer(invocation -> {
                UserHolding holding = invocation.getArgument(0);
                holdings.put(holding.getId(), holding);
                return holding;
            });
            when(userHoldingRepository.findByIdUserId(anyString())).thenAnswer(invocation -> {
                String userId = invocation.getArgument(0);
                return holdings.values().stream()
                        .filter(holding -> userId.equals(holding.getId().getUserId()))
                        .toList();
            });
            when(processedEventRepository.existsByEventIdAndEventType(anyString(), anyString()))
                    .thenAnswer(invocation -> processedEvents.contains(
                            invocation.getArgument(1) + ":" + invocation.getArgument(0)));
            when(processedEventRepository.save(any(ProcessedEvent.class))).thenAnswer(invocation -> {
                ProcessedEvent event = invocation.getArgument(0);
                processedEvents.add(event.getEventType() + ":" + event.getEventId());
                return event;
            });
            when(ledgerEntryRepository.save(any(LedgerEntryEntity.class)))
                    .thenAnswer(invocation -> invocation.getArgument(0));
        }

        void reset() {
            balances.clear();
            holdings.clear();
            processedEvents.clear();
        }
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    @Import({
            org.vivek.matchingengine.config.KafkaProducerConfig.class
    })
    static class IntegrationTestConfig {

        @Bean
        TrackingOrderRepository orderRepository() {
            return new TrackingOrderRepository();
        }

        @Bean
        SymbolOrderBook orderBook() {
            return new SymbolOrderBook("INFY");
        }

        @Bean
        InMemoryLedger inMemoryLedger() {
            return new InMemoryLedger();
        }

        @Bean
        LedgerConsumer ledgerConsumer(InMemoryLedger state) {
            LedgerConsumer consumer = new LedgerConsumer();
            ReflectionTestUtils.setField(consumer, "userBalanceRepository", state.userBalanceRepository);
            ReflectionTestUtils.setField(consumer, "userHoldingRepository", state.userHoldingRepository);
            ReflectionTestUtils.setField(consumer, "ledgerEntryRepository", state.ledgerEntryRepository);
            ReflectionTestUtils.setField(consumer, "processedEventRepository", state.processedEventRepository);
            return consumer;
        }

        @Bean
        InProcessMatchingEngineClient inProcessMatchingEngineClient(
                SymbolOrderBook orderBook,
                KafkaTemplate<String, TradeExecution> tradeKafkaTemplate,
                KafkaTemplate<String, CancellationEvent> cancellationKafkaTemplate
        ) {
            return new InProcessMatchingEngineClient(orderBook, tradeKafkaTemplate, cancellationKafkaTemplate);
        }

        @Bean
        @Primary
        MatchingEngineClient matchingEngineClient(InProcessMatchingEngineClient client) {
            return client;
        }

        @Bean
        @Primary
        RiskServiceGrpc.RiskServiceFutureStub riskServiceFutureStub() {
            ensureGrpcStarted();
            return RiskServiceGrpc.newFutureStub(RISK_GRPC.getChannel());
        }

        @Bean
        @Primary
        MarginServiceGrpc.MarginServiceFutureStub marginServiceFutureStub() {
            ensureGrpcStarted();
            return MarginServiceGrpc.newFutureStub(MARGIN_GRPC.getChannel());
        }

        @Bean
        @Primary
        ComplianceServiceGrpc.ComplianceServiceFutureStub complianceServiceFutureStub() {
            ensureGrpcStarted();
            return ComplianceServiceGrpc.newFutureStub(COMPLIANCE_GRPC.getChannel());
        }

        @Bean
        DAGExecutor dagExecutor(
                RiskServiceGrpc.RiskServiceFutureStub riskStub,
                MarginServiceGrpc.MarginServiceFutureStub marginStub,
                ComplianceServiceGrpc.ComplianceServiceFutureStub complianceStub,
                ApplicationEventPublisher applicationEventPublisher,
                @Lazy DAGExecutor self
        ) {
            return new DAGExecutor(riskStub, marginStub, complianceStub, applicationEventPublisher, self);
        }

        @Bean
        OrderService orderService(
                TrackingOrderRepository orderRepository,
                DAGExecutor dagExecutor,
                MatchingEngineClient matchingEngineClient
        ) {
            return new OrderService(orderRepository, dagExecutor, matchingEngineClient);
        }

        @Bean
        ConsumerFactory<String, Object> consumerFactory() {
            Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "ledger-group");
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
            config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
            config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
            return new DefaultKafkaConsumerFactory<>(config);
        }

        @Bean
        ProducerFactory<String, Object> dltProducerFactory() {
            Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
            return new DefaultKafkaProducerFactory<>(config);
        }

        @Bean
        KafkaTemplate<String, Object> dltKafkaTemplate() {
            return new KafkaTemplate<>(dltProducerFactory());
        }

        @Bean
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaOperations<String, Object> dltKafkaTemplate) {
            DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                    dltKafkaTemplate,
                    (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition())
            );
            recoverer.setHeadersFunction((record, ex) -> {
                RecordHeaders headers = new RecordHeaders();
                headers.add("X-Exception-Message", String.valueOf(ex.getMessage()).getBytes(StandardCharsets.UTF_8));
                return headers;
            });
            return recoverer;
        }

        @Bean
        CommonErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
            return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
        }

        @Bean
        ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
                ConsumerFactory<String, Object> consumerFactory,
                CommonErrorHandler kafkaErrorHandler
        ) {
            ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.setCommonErrorHandler(kafkaErrorHandler);
            return factory;
        }
    }
}
