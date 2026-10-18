package org.vivek.ledgerservice;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.vivek.commonmodule.model.CancellationEvent;
import org.vivek.commonmodule.model.TradeExecution;
import org.vivek.ledgerservice.entity.LedgerEntryEntity;
import org.vivek.ledgerservice.entity.ProcessedEvent;
import org.vivek.ledgerservice.entity.UserBalance;
import org.vivek.ledgerservice.entity.UserHolding;
import org.vivek.ledgerservice.entity.UserHoldingId;
import org.vivek.ledgerservice.repository.LedgerEntryRepository;
import org.vivek.ledgerservice.repository.ProcessedEventRepository;
import org.vivek.ledgerservice.repository.UserBalanceRepository;
import org.vivek.ledgerservice.repository.UserHoldingRepository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LedgerConsumerTest {

    @Mock
    private UserBalanceRepository userBalanceRepository;

    @Mock
    private UserHoldingRepository userHoldingRepository;

    @Mock
    private LedgerEntryRepository ledgerEntryRepository;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @InjectMocks
    private LedgerConsumer consumer;

    private final Map<String, UserBalance> balances = new HashMap<>();
    private final Map<UserHoldingId, UserHolding> holdings = new HashMap<>();
    private final Set<String> processedEvents = new HashSet<>();

    @BeforeEach
    void setUp() {
        balances.clear();
        holdings.clear();
        processedEvents.clear();

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

    @Test
    void consumeCancellationRecordsEventWithoutChangingCashBalance() {
        balances.put("user-1", new UserBalance("user-1", BigDecimal.valueOf(250.0d), Instant.now()));

        consumer.consumeCancellation(CancellationEvent.builder()
                .orderId("ORD-1")
                .userId("user-1")
                .symbol("AAPL")
                .cancelledAt(Instant.now())
                .build());

        assertEquals(250.0d, consumer.getBalance("user-1").get("balance"));
        assertEquals(true, processedEvents.contains("ORDER_CANCELLED:ORD-1"));
    }

    @Test
    void consumeTradeExecutionUpdatesBuyerAndSellerBalances() {
        consumer.consumeTradeExecution(trade());

        assertEquals(-200.0d, consumer.getBalance("buyer").get("balance"));
        assertEquals(200.0d, consumer.getBalance("seller").get("balance"));
        assertEquals(2.0d, consumer.getHoldings("buyer").get("AAPL"));
        assertEquals(-2.0d, consumer.getHoldings("seller").get("AAPL"));
    }

    @Test
    void duplicateTradeIsIgnored() {
        TradeExecution trade = trade();

        consumer.consumeTradeExecution(trade);
        consumer.consumeTradeExecution(trade);

        assertEquals(-200.0d, consumer.getBalance("buyer").get("balance"));
        assertEquals(200.0d, consumer.getBalance("seller").get("balance"));
        assertEquals(2.0d, consumer.getHoldings("buyer").get("AAPL"));
        assertEquals(-2.0d, consumer.getHoldings("seller").get("AAPL"));
    }

    private static TradeExecution trade() {
        return TradeExecution.builder()
                .tradeId("TRD-1")
                .buyOrderId("BUY-1")
                .sellOrderId("SELL-1")
                .buyerId("buyer")
                .sellerId("seller")
                .symbol("AAPL")
                .quantity(2.0d)
                .executedPrice(100.0d)
                .executedAt(Instant.now())
                .build();
    }
}
