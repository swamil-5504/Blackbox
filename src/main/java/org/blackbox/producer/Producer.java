package org.blackbox.producer;

import org.blackbox.message.Message;
import org.blackbox.storage.PartitionLog;
import org.blackbox.topic.TopicManager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class Producer {

    private final TopicManager topicManager;

    private final AtomicInteger nextPartition =
            new AtomicInteger(0);

    public Producer (TopicManager topicManager){
        this.topicManager = topicManager;
    }

    public long send(String topicName, Message message)
            throws IOException {

        return sendWithResult(topicName, message).offset();
    }

    public SendResult sendWithResult(
            String topicName,
            Message message
    ) throws IOException {

        int partitionCount =
                topicManager.getPartitionCount(topicName);

        int partitionNumber = Math.floorMod(
                nextPartition.getAndIncrement(),
                partitionCount
        );

        PartitionLog partitionLog =
                topicManager.getPartitionLog(
                        topicName,
                        partitionNumber
                );

        long offset = partitionLog.append(message);

        return new SendResult(partitionNumber, offset);
    }

    public record SendResult(int partition, long offset) {
    }

}
