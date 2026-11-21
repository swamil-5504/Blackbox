package org.blackbox.network;
import org.blackbox.topic.TopicManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.blackbox.consumer.Consumer;
import org.blackbox.message.Message;
import org.blackbox.producer.Producer;
import org.blackbox.consumer.ConsumerGroupManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlackboxServer {

    private final int port;
    private final Producer producer;
    private final Consumer consumer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TopicManager topicManager;
    private final ConsumerGroupManager groupManager;


    public BlackboxServer(
            int port,
            Producer producer,
            Consumer consumer,
            TopicManager topicManager,
            ConsumerGroupManager groupManager
    ) {
        this.port = port;
        this.producer = producer;
        this.consumer = consumer;
        this.topicManager = topicManager;
        this.groupManager = groupManager;
    }

    public void start() throws IOException {

        try (ServerSocket serverSocket = new ServerSocket(port)) {

            System.out.println(
                    "BLACKBOX server listening on port " + port
            );

            while (true) {
                Socket clientSocket = serverSocket.accept();

                System.out.println(
                        "Client connected: "
                                + clientSocket.getRemoteSocketAddress()
                );

                Thread clientThread = new Thread(
                        () -> handleClient(clientSocket)
                );

                clientThread.start();
            }
        }
    }

    private void handleClient(Socket clientSocket) {

        try (
                Socket socket = clientSocket;
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                                socket.getInputStream(),
                                StandardCharsets.UTF_8
                        )
                );
                PrintWriter writer = new PrintWriter(
                        socket.getOutputStream(),
                        true,
                        StandardCharsets.UTF_8
                )
        ) {
            String line;

            // Each line from the client must contain one JSON request.
            while ((line = reader.readLine()) != null) {

                try {
                    JsonNode request = objectMapper.readTree(line);

                    Map<String, Object> response =
                            processRequest(request);

                    writer.println(
                            objectMapper.writeValueAsString(response)
                    );

                } catch (Exception e) {

                    Map<String, Object> error = new HashMap<>();

                    error.put("status", "ERROR");
                    error.put(
                            "message",
                            e.getMessage() == null
                                    ? "Request failed"
                                    : e.getMessage()
                    );

                    writer.println(
                            objectMapper.writeValueAsString(error)
                    );
                }
            }

        } catch (IOException e) {
            System.err.println(
                    "Client connection error: " + e.getMessage()
            );
        }

        System.out.println("Client disconnected.");
    }

    private Map<String, Object> processRequest(
            JsonNode request
    ) throws IOException {

        String command = requiredText(request, "command");

        switch (command.toUpperCase()) {

            case "PUBLISH": {
                String topic = requiredText(request, "topic");
                String value = requiredText(request, "value");

                String key = null;

                if (request.hasNonNull("key")) {
                    if (!request.get("key").isTextual()) {
                        throw new IllegalArgumentException(
                                "'key' must be a string or null"
                        );
                    }

                    key = request.get("key").asText();
                }

                Producer.SendResult result = producer.sendWithResult(
                        topic,
                        Message.create(key,value)
                );

                Map<String, Object> response = new HashMap<>();

                response.put("status", "OK");
                response.put("partition",result.partition());
                response.put("offset", result.offset());

                return response;
            }

            case "FETCH": {
                String topic = requiredText(request, "topic");

                int partition = requiredInt(request, "partition");
                long offset = requiredLong(request, "offset");
                int limit = requiredInt(request, "limit");

                List<Message> messages = consumer.poll(
                        topic,
                        partition,
                        offset,
                        limit
                );

                Map<String, Object> response = new HashMap<>();

                response.put("status", "OK");
                response.put("messages", messages);

                return response;
            }

            case "FETCH_GROUP": {

                String groupId = requiredText(request, "groupId");
                String consumerId = requiredText(request, "consumerId");
                String topic = requiredText(request, "topic");

                int partition = requiredInt(request, "partition");
                int limit = requiredInt(request, "limit");


                var group = groupManager.getGroup(groupId);


                if (!group.getMembers().contains(consumerId)) {
                    throw new IllegalArgumentException(
                            "Consumer is not a member of this group: " + consumerId
                    );
                }


                Map<String, List<Integer>> assignments =
                        group.getAssignment(topic);

                List<Integer> assignedPartitions = assignments.get(consumerId);

                if (assignedPartitions == null) {
                    throw new IllegalArgumentException(
                            "Consumer group is not subscribed to topic: " + topic
                    );
                }


                if (!assignedPartitions.contains(partition)) {
                    throw new IllegalArgumentException(
                            "Consumer " + consumerId
                                    + " is not assigned partition " + partition
                    );
                }


                List<Message> messages = consumer.pollGroup(
                        groupId,
                        topic,
                        partition,
                        limit
                );

                Map<String, Object> response = new HashMap<>();

                response.put("status", "OK");
                response.put("groupId", groupId);
                response.put("consumerId", consumerId);
                response.put("topic", topic);
                response.put("partition", partition);
                response.put("messages", messages);

                return response;
            }

            case "COMMIT_OFFSET": {

                String groupId = requiredText(request, "groupId");
                String topic = requiredText(request, "topic");

                int partition = requiredInt(request, "partition");
                long offset = requiredLong(request, "offset");

                consumer.commitGroupOffset(
                        groupId,
                        topic,
                        partition,
                        offset
                );

                Map<String, Object> response = new HashMap<>();

                response.put("status", "OK");
                response.put("message", "Offset committed successfully");
                response.put("groupId", groupId);
                response.put("topic", topic);
                response.put("partition", partition);
                response.put("offset", offset);

                return response;
            }

            case "CREATE_TOPIC": {

                String topic = requiredText(request, "topic");

                int partitions = requiredInt(request, "partitions");

                topicManager.createTopic(topic, partitions);

                Map<String, Object> response = new HashMap<>();

                response.put("status", "OK");
                response.put("message", "Topic created successfully");
                response.put("topic", topic);
                response.put("partitions", partitions);

                return response;
            }

            case "CREATE_GROUP": {
                String groupId = requiredText(request,"groupId");

                groupManager.createGroup(groupId);

                Map<String,Object> response = new HashMap<>();

                response.put("status","OK");
                response.put("message", "Consumer group created successfully");
                response.put("groupId",groupId);

                return response;
            }

            case "JOIN_GROUP": {

                String groupId = requiredText(request, "groupId");
                String consumerId = requiredText(request, "consumerId");

                groupManager.registerConsumer(groupId, consumerId);

                Map<String, Object> response = new HashMap<>();

                response.put("status", "OK");
                response.put("message", "Consumer joined group successfully");
                response.put("groupId", groupId);
                response.put("consumerId", consumerId);

                return response;
            }

            case "LEAVE_GROUP": {

                String groupId = requiredText(request, "groupId");
                String consumerId = requiredText(request, "consumerId");

                groupManager.removeConsumer(groupId, consumerId);

                Map<String, Object> response = new HashMap<>();

                response.put("status", "OK");
                response.put("message", "Consumer left group successfully");
                response.put("groupId", groupId);
                response.put("consumerId", consumerId);

                return response;
            }

            case "SUBSCRIBE": {

                String groupId = requiredText(request, "groupId");
                String topic = requiredText(request, "topic");

                groupManager.subscribeTopic(groupId, topic);

                Map<String, Object> response = new HashMap<>();

                response.put("status", "OK");
                response.put("message", "Topic subscription successful");
                response.put("groupId", groupId);
                response.put("topic", topic);
                response.put(
                        "partitions",
                        topicManager.getPartitionCount(topic)
                );

                return response;
            }

            case "GET_ASSIGNMENTS": {

                String groupId = requiredText(request, "groupId");
                String consumerId = requiredText(request, "consumerId");
                String topic = requiredText(request, "topic");

                Map<String, List<Integer>> assignments =
                        groupManager.getGroup(groupId).getAssignment(topic);

                List<Integer> partitions = assignments.get(consumerId);

                if (partitions == null) {
                    throw new IllegalArgumentException(
                            "Consumer is not a member of this group: " + consumerId
                    );
                }

                Map<String, Object> response = new HashMap<>();

                response.put("status", "OK");
                response.put("groupId", groupId);
                response.put("consumerId", consumerId);
                response.put("topic", topic);
                response.put("partitions", partitions);

                return response;
            }

            default:
                throw new IllegalArgumentException(
                        "Unknown command: " + command
                );
        }
    }

    private String requiredText(JsonNode request, String field) {

        JsonNode value = request.get(field);

        if (value == null
                || !value.isTextual()
                || value.asText().isBlank()) {
            throw new IllegalArgumentException(
                    "Missing or invalid field: " + field
            );
        }

        return value.asText();
    }

    private int requiredInt(JsonNode request, String field) {

        JsonNode value = request.get(field);

        if (value == null || !value.canConvertToInt()) {
            throw new IllegalArgumentException(
                    "Missing or invalid integer field: " + field
            );
        }

        return value.intValue();
    }

    private long requiredLong(JsonNode request, String field) {

        JsonNode value = request.get(field);

        if (value == null || !value.canConvertToLong()) {
            throw new IllegalArgumentException(
                    "Missing or invalid long field: " + field
            );
        }

        return value.longValue();
    }
}