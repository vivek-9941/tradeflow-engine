package org.vivek.marginservice.service;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.vivek.commonmodule.model.CancellationEvent;
import org.vivek.commonmodule.model.TradeExecution;
import org.vivek.marginservice.entity.MarginAccount;
import org.vivek.marginservice.entity.OrderReservation;
import org.vivek.marginservice.repository.MarginAccountRepository;
import org.vivek.marginservice.repository.OrderReservationRepository;
import org.vivek.trade.margin.grpc.ValidationRequest;
import org.vivek.trade.margin.grpc.ValidationResponse;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MarginServiceImplTest {

    @Mock
    private MarginAccountRepository marginAccountRepository;

    @Mock
    private OrderReservationRepository orderReservationRepository;

    @InjectMocks
    private MarginServiceImpl marginService;

    private final Map<String, MarginAccount> accounts = new HashMap<>();
    private final Map<String, OrderReservation> reservations = new HashMap<>();

    @BeforeEach
    void setUp() {
        accounts.clear();
        reservations.clear();
        accounts.put("U1", account("U1", 100000.0d, 150000.0d));
        accounts.put("U2", account("U2", 250000.0d, 0.0d));
        accounts.put("U3", account("U3", 50000.0d, 0.0d));

        when(marginAccountRepository.findById(anyString())).thenAnswer(invocation ->
                Optional.ofNullable(accounts.get(invocation.getArgument(0))));
        when(marginAccountRepository.save(any(MarginAccount.class))).thenAnswer(invocation -> {
            MarginAccount account = invocation.getArgument(0);
            accounts.put(account.getUserId(), account);
            return account;
        });
        when(orderReservationRepository.findByOrderId(anyString())).thenAnswer(invocation ->
                Optional.ofNullable(reservations.get(invocation.getArgument(0))));
        when(orderReservationRepository.save(any(OrderReservation.class))).thenAnswer(invocation -> {
            OrderReservation reservation = invocation.getArgument(0);
            reservations.put(reservation.getOrderId(), reservation);
            return reservation;
        });
        doAnswer(invocation -> reservations.remove(invocation.getArgument(0)))
                .when(orderReservationRepository).deleteByOrderId(anyString());
    }

    private static MarginAccount account(String userId, double cashBalance, double holdingsValue) {
        return new MarginAccount(userId, BigDecimal.valueOf(cashBalance),
                BigDecimal.valueOf(holdingsValue), BigDecimal.ZERO, Instant.now());
    }

    @Test
    void validateReservesMarginForDeliveryBuy() {
        ValidationResponse response = validate(request("ORD-1", "U1", "BUY", 10.0d, 1000.0d, "LIMIT"));

        assertTrue(response.getSuccess());
        assertTrue(response.getReason().startsWith("MARGIN_OK"));

        MarginServiceImpl.MarginSnapshot snapshot = marginService.getMarginSnapshot("U1");
        assertEquals(10000.0d, snapshot.reservedMargin(), 0.0001d);
        assertEquals(240000.0d, snapshot.availableMargin(), 0.0001d);
    }

    @Test
    void validateRejectsWhenMarginIsInsufficient() {
        ValidationResponse response = validate(request("ORD-2", "U3", "BUY", 100.0d, 1000.0d, "LIMIT"));

        assertFalse(response.getSuccess());
        assertTrue(response.getReason().contains("INSUFFICIENT_MARGIN"));

        MarginServiceImpl.MarginSnapshot snapshot = marginService.getMarginSnapshot("U3");
        assertEquals(0.0d, snapshot.reservedMargin(), 0.0001d);
    }

    @Test
    void intradayShortSellRequiresMargin() {
        ValidationResponse response = validate(request("ORD-3", "U3", "SELL", 500.0d, 1000.0d, "IOC"));

        assertFalse(response.getSuccess());
        assertTrue(response.getReason().contains("INSUFFICIENT_MARGIN_FOR_SHORT"));
    }

    @Test
    void tradeExecutionReleasesReservationAndMovesCash() {
        validate(request("BUY-1", "U1", "BUY", 10.0d, 1000.0d, "LIMIT"));
        validate(request("SELL-1", "U2", "SELL", 5.0d, 1000.0d, "IOC"));

        marginService.onTradeExecuted(TradeExecution.builder()
                .tradeId("TRD-1")
                .buyOrderId("BUY-1")
                .sellOrderId("SELL-1")
                .buyerId("U1")
                .sellerId("U2")
                .symbol("INFY")
                .quantity(10.0d)
                .executedPrice(1000.0d)
                .executedAt(Instant.now())
                .build());

        MarginServiceImpl.MarginSnapshot buyer = marginService.getMarginSnapshot("U1");
        MarginServiceImpl.MarginSnapshot seller = marginService.getMarginSnapshot("U2");

        assertEquals(90000.0d, buyer.cashBalance(), 0.0001d);
        assertEquals(260000.0d, seller.cashBalance(), 0.0001d);
        assertEquals(0.0d, buyer.reservedMargin(), 0.0001d);
        assertEquals(0.0d, seller.reservedMargin(), 0.0001d);
    }

    @Test
    void cancellationReleasesReservedMargin() {
        validate(request("ORD-4", "U2", "BUY", 10.0d, 500.0d, "LIMIT"));

        marginService.onOrderCancelled(CancellationEvent.builder()
                .orderId("ORD-4")
                .userId("U2")
                .symbol("TCS")
                .cancelledAt(Instant.now())
                .build());

        MarginServiceImpl.MarginSnapshot snapshot = marginService.getMarginSnapshot("U2");
        assertEquals(0.0d, snapshot.reservedMargin(), 0.0001d);
    }

    @Test
    void depositAddsCashBalance() {
        MarginServiceImpl.MarginSnapshot snapshot = marginService.deposit("U3", 25000.0d);

        assertEquals(75000.0d, snapshot.cashBalance(), 0.0001d);
        assertEquals(75000.0d, snapshot.availableMargin(), 0.0001d);
    }

    @Test
    void withdrawReducesCashBalance() {
        MarginServiceImpl.MarginSnapshot snapshot = marginService.withdraw("U1", 10000.0d);

        assertEquals(90000.0d, snapshot.cashBalance(), 0.0001d);
        assertEquals(240000.0d, snapshot.availableMargin(), 0.0001d);
    }

    @Test
    void withdrawFailsWhenCashIsInsufficient() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> marginService.withdraw("U3", 60000.0d)
        );

        assertTrue(exception.getMessage().contains("INSUFFICIENT_FUNDS"));
    }

    private ValidationRequest request(
            String orderId,
            String userId,
            String side,
            double quantity,
            double price,
            String orderType) {
        return ValidationRequest.newBuilder()
                .setOrderId(orderId)
                .setUserId(userId)
                .setSymbol("INFY")
                .setSide(side)
                .setQuantity(quantity)
                .setPrice(price)
                .setOrderType(orderType)
                .build();
    }

    private ValidationResponse validate(ValidationRequest request) {
        TestObserver observer = new TestObserver();
        marginService.validate(request, observer);
        assertNotNull(observer.response);
        return observer.response;
    }

    private static final class TestObserver implements StreamObserver<ValidationResponse> {
        private ValidationResponse response;
        private Throwable error;

        @Override
        public void onNext(ValidationResponse value) {
            this.response = value;
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
        }

        @Override
        public void onCompleted() {
            if (error != null) {
                throw new AssertionError("Unexpected observer error", error);
            }
        }
    }
}
