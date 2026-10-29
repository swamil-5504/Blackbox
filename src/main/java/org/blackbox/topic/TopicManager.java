package org.blackbox.topic;

import org.blackbox.storage.PartitionLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TopicManager implements AutoCloseable {

    private final Path dataDirectory;

    // Stores each topic and its partition logs.
    private final Map<String, List<PartitionLog>> topics =
            new HashMap<>();

    public TopicManager(Path dataDirectory) throws IOException {
        this.dataDirectory = dataDirectory;
        Files.createDirectories(dataDirectory);
    }

    // Create a topic with the requested number of partitions.
    public synchronized void createTopic(
            String topicName,
            int partitionCount
    ) throws IOException {

        if (topicName == null
                || !topicName.matches("[a-zA-Z0-9._-]+")
                || topicName.equals(".")
                || topicName.equals("..")) {
            throw new IllegalArgumentException("Invalid topic name");
        }

        if (partitionCount <= 0) {
            throw new IllegalArgumentException(
                    "Partition count must be greater than zero"
            );
        }

        if (topics.containsKey(topicName)) {
            throw new IllegalArgumentException(
                    "Topic already exists: " + topicName
            );
        }

        List<PartitionLog> partitions = new ArrayList<>();

        try {
            for (int i = 0; i < partitionCount; i++) {

                Path logPath = dataDirectory
                        .resolve(topicName)
                        .resolve("partition-" + i + ".log");

                PartitionLog partitionLog =
                        new PartitionLog(logPath);

                partitions.add(partitionLog);
            }

            topics.put(topicName, partitions);

            System.out.println(
                    "Created topic: " + topicName
                            + " with " + partitionCount
                            + " partition(s)"
            );

        } catch (IOException | RuntimeException e) {
            for (PartitionLog partition : partitions) {
                try {
                    partition.close();
                } catch (IOException closeError) {
                    e.addSuppressed(closeError);
                }
            }

            throw e;
        }
    }

    // Find a specific partition of a topic.
    public synchronized PartitionLog getPartitionLog(
            String topicName,
            int partitionNumber
    ) {

        List<PartitionLog> partitions = topics.get(topicName);

        if (partitions == null) {
            throw new IllegalArgumentException(
                    "Topic does not exist: " + topicName
            );
        }

        if (partitionNumber < 0
                || partitionNumber >= partitions.size()) {
            throw new IllegalArgumentException(
                    "Invalid partition number: " + partitionNumber
            );
        }

        return partitions.get(partitionNumber);
    }

    public synchronized int getPartitionCount(String topicName){
        List<PartitionLog> partitions = topics.get(topicName);

        if(partitions == null){
            throw new IllegalArgumentException(
                    "Topic does not exist: " + topicName
            );
        }

        return partitions.size();
    }

    // Return the names of topics currently managed by this instance.
    public synchronized Set<String> listTopics() {
        return Set.copyOf(topics.keySet());
    }

    @Override
    public synchronized void close() throws IOException {

        IOException failure = null;

        for (List<PartitionLog> partitions : topics.values()) {
            for (PartitionLog partition : partitions) {
                try {
                    partition.close();
                } catch (IOException e) {
                    if (failure == null) {
                        failure = new IOException(
                                "Failed to close one or more partition logs"
                        );
                    }

                    failure.addSuppressed(e);
                }
            }
        }

        topics.clear();

        if (failure != null) {
            throw failure;
        }
    }
}