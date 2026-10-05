package org.vivek.matchingengine.orderbook;

import org.openjdk.jmh.annotations.*;
import org.vivek.commonmodule.model.Order;
import org.vivek.commonmodule.model.OrderSide;
import org.vivek.commonmodule.model.OrderType;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G", "-XX:+UseG1GC"})
public class OrderBookBenchmark {

    private SymbolOrderBook orderBook;
    private List<Command> commands;
    private int commandIndex;

    static class Command {
        boolean isCancel;
        String orderId;
        String userId;
        OrderSide side;
        OrderType type;
        double price;
        double qty;
    }

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        String workloadFile = System.getProperty("workload.file", "workload.bin");
        commands = new ArrayList<>();
        
        try (DataInputStream dis = new DataInputStream(new FileInputStream(workloadFile))) {
            while (dis.available() > 0) {
                Command cmd = new Command();
                cmd.isCancel = dis.readByte() == 1;
                
                byte[] oidBytes = new byte[48];
                dis.readFully(oidBytes);
                cmd.orderId = new String(oidBytes).trim();
                
                byte[] uidBytes = new byte[32];
                dis.readFully(uidBytes);
                cmd.userId = new String(uidBytes).trim();
                
                cmd.side = dis.readByte() == 0 ? OrderSide.BUY : OrderSide.SELL;
                
                byte typeByte = dis.readByte();
                cmd.type = typeByte == 0 ? OrderType.LIMIT : (typeByte == 1 ? OrderType.IOC : OrderType.GTD);
                
                cmd.price = Long.reverseBytes(dis.readLong()) / 100.0;
                cmd.qty = Long.reverseBytes(dis.readLong());
                
                dis.readLong(); // ts_ns (skip)
                dis.readLong(); // seq (skip)
                
                // Note: Little-endian vs Big-endian. C++ writes little-endian (typically). 
                // DataInputStream reads Big-endian. We used Long.reverseBytes.
                
                commands.add(cmd);
            }
        }
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        orderBook = new SymbolOrderBook("sym1", null, null); // Provide null for Kafka templates as we only want to measure core matching
        commandIndex = 0;
    }

    @Benchmark
    public void replayWorkload() {
        if (commandIndex >= commands.size()) {
            return;
        }
        Command cmd = commands.get(commandIndex++);
        
        if (cmd.isCancel) {
            orderBook.cancelOrder(cmd.orderId);
        } else {
            Order order = new Order();
            order.setOrderId(cmd.orderId);
            order.setUserId(cmd.userId);
            order.setSymbol("sym1");
            order.setOrderSide(cmd.side);
            order.setOrderType(cmd.type);
            order.setPrice(cmd.price);
            order.setQuantity(cmd.qty);
            
            orderBook.match(order);
        }
    }
}
