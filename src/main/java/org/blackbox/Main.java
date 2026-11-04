
package org.blackbox;

import org.blackbox.consumer.Consumer;
import org.blackbox.consumer.OffsetManager;
import org.blackbox.producer.Producer;
import org.blackbox.topic.TopicManager;
import org.blackbox.network.BlackboxServer;

import java.nio.file.Path;

public class Main {

    public static void main(String[] args) throws Exception {

        Path dataDirectory = Path.of("data");

        try (TopicManager topicManager =
                     new TopicManager(dataDirectory)) {

            OffsetManager offsetManager =
                    new OffsetManager(Path.of("offsets"));

            Producer producer = new Producer(topicManager);

            Consumer consumer = new Consumer(
                    topicManager,
                    offsetManager
            );

            BlackboxServer server = new BlackboxServer(
                    9092,
                    producer,
                    consumer,
                    topicManager
            );

            server.start();
        }
    }
}
