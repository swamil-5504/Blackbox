
package org.blackbox.consumer;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConsumerGroup {


    private final String groupId;


    private final Set<String> members = new LinkedHashSet<>();


    private final Map<String, Map<String, List<Integer>>> assignments =
            new LinkedHashMap<>();

    public ConsumerGroup(String groupId) {

        if (groupId == null ||
                !groupId.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("Invalid group ID");
        }

        this.groupId = groupId;
    }


    public synchronized boolean addMember(String consumerId) {

        if (consumerId == null || consumerId.isBlank()) {
            throw new IllegalArgumentException("Invalid consumer ID");
        }

        return members.add(consumerId);
    }


    public synchronized boolean removeMember(String consumerId) {
        return members.remove(consumerId);
    }


    public synchronized List<String> getMembers() {
        return List.copyOf(members);
    }


    public String getGroupId() {
        return groupId;
    }


    public synchronized int getMemberCount() {
        return members.size();
    }


    public synchronized void setAssignment(
            String topicName,
            Map<String, List<Integer>> topicAssignments
    ) {

        if (topicName == null ||
                !topicName.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("Invalid topic name");
        }

        if (topicAssignments == null) {
            throw new IllegalArgumentException(
                    "Assignments cannot be null"
            );
        }

        Map<String, List<Integer>> copy = new LinkedHashMap<>();

        for (Map.Entry<String, List<Integer>> entry :
                topicAssignments.entrySet()) {

            String consumerId = entry.getKey();
            List<Integer> partitions = entry.getValue();

            if (!members.contains(consumerId)) {
                throw new IllegalArgumentException(
                        "Consumer is not a member: " + consumerId
                );
            }

            if (partitions == null) {
                throw new IllegalArgumentException(
                        "Partition list cannot be null"
                );
            }

            copy.put(consumerId, List.copyOf(partitions));
        }

        assignments.put(topicName, copy);
    }


    public synchronized Map<String, List<Integer>> getAssignment(
            String topicName
    ) {

        Map<String, List<Integer>> topicAssignments =
                assignments.get(topicName);

        if (topicAssignments == null) {
            return Map.of();
        }

        return Map.copyOf(topicAssignments);
    }
}
