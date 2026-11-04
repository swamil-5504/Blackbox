
package org.blackbox.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class OffsetManager {

    private final Path offsetDirectory;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public OffsetManager(Path offsetDirectory) throws IOException {
        this.offsetDirectory = offsetDirectory;

        Files.createDirectories(offsetDirectory);
    }

    private Path getOffsetFile(String groupId) {
        return offsetDirectory.resolve(groupId + ".json");
    }

    public synchronized void commitOffset(
            String groupId,
            String topicName,
            int partition,
            long nextOffset
    ) throws IOException {

        if (groupId == null || !groupId.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("Invalid group ID");
        }

        if (topicName == null || !topicName.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("Invalid topic name");
        }

        if (partition < 0 || nextOffset < 0) {
            throw new IllegalArgumentException("Invalid partition or offset");
        }

        Path offsetFile = getOffsetFile(groupId);

        Map<String, Map<Integer, Long>> offsets;

        if (Files.exists(offsetFile)) {
            offsets = objectMapper.readValue(
                    offsetFile.toFile(),
                    new TypeReference<>() {}
            );
        } else {
            offsets = new HashMap<>();
        }

        offsets.putIfAbsent(topicName, new HashMap<>());

        offsets.get(topicName).put(partition, nextOffset);

        objectMapper.writerWithDefaultPrettyPrinter()
                .writeValue(offsetFile.toFile(), offsets);
    }

    public synchronized long getCommittedOffset(
            String groupId,
            String topicName,
            int partition
    ) throws IOException {

        if (groupId == null || !groupId.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("Invalid group ID");
        }

        if (topicName == null || !topicName.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("Invalid topic name");
        }

        if (partition < 0) {
            throw new IllegalArgumentException("Invalid partition");
        }

        Path offsetFile = getOffsetFile(groupId);

        if (!Files.exists(offsetFile)) {
            return 0;
        }

        Map<String, Map<Integer, Long>> offsets =
                objectMapper.readValue(
                        offsetFile.toFile(),
                        new TypeReference<>() {}
                );

        return offsets
                .getOrDefault(topicName, Map.of())
                .getOrDefault(partition, 0L);
    }
}
