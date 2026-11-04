
package org.blackbox.consumer;

import org.blackbox.message.Message;
import org.blackbox.storage.PartitionLog;
import org.blackbox.topic.TopicManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Consumer {

    private final TopicManager topicManager;
    private final OffsetManager offsetManager;

    public Consumer(
            TopicManager topicManager,
            OffsetManager offsetManager
    ) {
        this.topicManager = topicManager;
        this.offsetManager = offsetManager;
    }

    // Existing method: manually specify the starting offset
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

        return new ArrayList<>(
                allMessages.subList(start, end)
        );
    }


    public List<Message> pollGroup(
            String groupId,
            String topicName,
            int partitionNumber,
            int maxMessages
    ) throws IOException {

        long committedOffset = offsetManager.getCommittedOffset(
                groupId,
                topicName,
                partitionNumber
        );

        return poll(
                topicName,
                partitionNumber,
                committedOffset,
                maxMessages
        );
    }


    public void commitGroupOffset(
            String groupId,
            String topicName,
            int partitionNumber,
            long nextOffset
    ) throws IOException {

        if (nextOffset < 0) {
            throw new IllegalArgumentException(
                    "Offset cannot be negative"
            );
        }

        PartitionLog partitionLog =
                topicManager.getPartitionLog(
                        topicName,
                        partitionNumber
                );

        long messageCount = partitionLog.readAll().size();

        if (nextOffset > messageCount) {
            throw new IllegalArgumentException(
                    "Cannot commit offset " + nextOffset
                            + ". Maximum valid offset is "
                            + messageCount
            );
        }

        offsetManager.commitOffset(
                groupId,
                topicName,
                partitionNumber,
                nextOffset
        );
    }
}
