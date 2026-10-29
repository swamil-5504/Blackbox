package org.blackbox;

import org.blackbox.consumer.Consumer;
import org.blackbox.producer.Producer;
import org.blackbox.topic.TopicManager;
import org.blackbox.network.BlackboxServer;

import java.nio.file.Path;

public class Main {

    public static void main(String[] args) throws Exception {

        Path dataDirectory = Path.of("data");

        try (TopicManager topicManager =
                     new TopicManager(dataDirectory)) {

            // Create the topic and its three partitions.
            topicManager.createTopic("orders", 3);

            // Create the Producer and Consumer.
            Producer producer = new Producer(topicManager);
            Consumer consumer = new Consumer(topicManager);

            // Start the TCP server on port 9092.
            BlackboxServer server = new BlackboxServer(
                    9092,
                    producer,
                    consumer
            );

            server.start();
        }
    }
}