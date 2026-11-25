
package org.blackbox;

import org.blackbox.cluster.ClusterConfig;
import org.blackbox.cluster.InterBrokerClient;
import org.blackbox.cluster.InterBrokerServer;
import org.blackbox.consumer.Consumer;
import org.blackbox.consumer.ConsumerGroupManager;
import org.blackbox.consumer.OffsetManager;
import org.blackbox.network.BlackboxServer;
import org.blackbox.producer.Producer;
import org.blackbox.topic.TopicManager;

import java.nio.file.Path;

public class Main {

    public static void main(String[] args) throws Exception {

        String brokerId = args.length > 0
                ? args[0]
                : "broker-1";

        int port = args.length > 1
                ? Integer.parseInt(args[1])
                : 9092;

        Path dataDirectory = args.length > 2
                ? Path.of(args[2])
                : Path.of("data");

        Path offsetDirectory = args.length > 3
                ? Path.of(args[3])
                : Path.of("offsets");


        ClusterConfig clusterConfig =
                ClusterConfig.load("config/cluster.properties");


        ClusterConfig.BrokerAddress localBroker =
                clusterConfig.getBroker(brokerId);

        int internalPort = localBroker.internalPort();


        if (localBroker.port() != port) {
            throw new IllegalArgumentException(
                    "Configured port for " + brokerId
                            + " is " + localBroker.port()
                            + ", but startup port is " + port
            );
        }

        System.out.println("Starting BLACKBOX broker...");
        System.out.println("Broker ID: " + brokerId);
        System.out.println("Port: " + port);
        System.out.println("Internal port: " + internalPort);
        System.out.println("Data directory: " + dataDirectory);
        System.out.println("Offset directory: " + offsetDirectory);
        System.out.println("Cluster: " + clusterConfig.getClusterName());
        System.out.println("Known brokers: " + clusterConfig.getBrokers());

        try (TopicManager topicManager =
                     new TopicManager(dataDirectory)) {

            OffsetManager offsetManager =
                    new OffsetManager(offsetDirectory);

            Producer producer = new Producer(topicManager);

            Consumer consumer =
                    new Consumer(topicManager, offsetManager);

            ConsumerGroupManager groupManager =
                    new ConsumerGroupManager(topicManager);


            BlackboxServer server = new BlackboxServer(
                    port,
                    producer,
                    consumer,
                    topicManager,
                    groupManager
            );


            InterBrokerServer interBrokerServer =
                    new InterBrokerServer(
                            brokerId,
                            clusterConfig.getClusterName(),
                            internalPort
                    );

            Thread interBrokerThread = new Thread(() -> {

                try {
                    interBrokerServer.start();

                } catch (Exception e) {
                    System.err.println(
                            "Inter-broker server failed: "
                                    + e.getMessage()
                    );
                }

            }, "inter-broker-" + brokerId);

            interBrokerThread.start();


            InterBrokerClient interBrokerClient =
                    new InterBrokerClient(
                            brokerId,
                            clusterConfig.getClusterName()
                    );


            Runtime.getRuntime().addShutdownHook(
                    new Thread(
                            interBrokerClient::close,
                            "inter-broker-shutdown-" + brokerId
                    )
            );


            Thread peerConnectorThread = new Thread(() -> {

                for (var entry :
                        clusterConfig.getBrokers().entrySet()) {

                    String remoteBrokerId = entry.getKey();


                    if (remoteBrokerId.equals(brokerId)) {
                        continue;
                    }

                    ClusterConfig.BrokerAddress remoteBroker =
                            entry.getValue();

                    try {

                        // Establish a persistent connection.
                        interBrokerClient.connectWithRetry(
                                remoteBrokerId,
                                remoteBroker.host(),
                                remoteBroker.internalPort(),
                                15,
                                2000
                        );


                        interBrokerClient.ping(remoteBrokerId);

                    } catch (InterruptedException e) {

                        Thread.currentThread().interrupt();
                        return;

                    } catch (Exception e) {

                        System.err.println(
                                "Communication with " + remoteBrokerId
                                        + " failed: " + e.getMessage()
                        );
                    }
                }

            }, "peer-connector-" + brokerId);

            peerConnectorThread.start();


            server.start();
        }
    }
}
