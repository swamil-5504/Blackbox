package org.blackbox;

import org.blackbox.message.Message;
import org.blackbox.storage.PartitionLog;

import java.nio.file.Path;

public class Main {

    public static void main(String[] args) throws Exception {

        Path logPath = Path.of("data", "orders-1.log");

        try (PartitionLog log = new PartitionLog(logPath)) {
            long offset1 = log.append(
                    Message.create("payment-101", "Payment completed")
            );

            long offset2 = log.append(
                    Message.create("order-202", "Order created")
            );

            long offset3 = log.append(
                    Message.create("order-6969","Order is almost finished")
            );

            System.out.println("First offset: " + offset1);
            System.out.println("Second offset: " + offset2);
            System.out.println("Third offset: " + offset3);

            System.out.println("\nStored messages:");

            for (Message message : log.readAll()) {
                System.out.println(message);
            }
        }

        // Reopen the same file to verify persistence.
        try (PartitionLog recoveredLog = new PartitionLog(logPath)) {
            long offset = recoveredLog.append(
                    Message.create("order-303", "Order shipped")
            );

            System.out.println("\nOffset after reopening: " + offset);
        }
    }
}