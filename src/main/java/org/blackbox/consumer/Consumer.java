package org.blackbox.consumer;

import org.blackbox.message.Message;
import org.blackbox.storage.PartitionLog;
import org.blackbox.topic.TopicManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Consumer {

    private final TopicManager topicManager;

    public Consumer(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    public List<Message> poll(
            String topicName,
            int partitionNumber,
            long startingOffset,
            int maxMessages
    ) throws IOException {

        if (startingOffset < 0) {
            throw new IllegalArgumentException(
                    "Starting offset cannot be negative"
            );
        }

        if (maxMessages <= 0) {
            throw new IllegalArgumentException(
                    "Maximum messages must be greater than zero"
            );
        }

        PartitionLog partitionLog =
                topicManager.getPartitionLog(
                        topicName,
                        partitionNumber
                );

        List<Message> allMessages = partitionLog.readAll();

        if (startingOffset >= allMessages.size()) {
            return List.of();
        }

        int start = Math.toIntExact(startingOffset);

        int end = (int) Math.min(
                (long) allMessages.size(),
                startingOffset + (long) maxMessages
        );

        return new ArrayList<>(allMessages.subList(start, end));
    }
}