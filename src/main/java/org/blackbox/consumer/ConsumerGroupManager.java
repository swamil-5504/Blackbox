package org.blackbox.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.blackbox.topic.TopicManager;
import java.util.Objects;

public class ConsumerGroupManager {

    private final Map<String, ConsumerGroup> groups = new HashMap<>();
    private final Map<String, Map<String,Integer>> subscriptions = new HashMap<>();
    private final TopicManager topicManager;
    private final PartitionAssignor assignor = new PartitionAssignor();

    public ConsumerGroupManager(TopicManager topicManager){
        this.topicManager = Objects.requireNonNull(topicManager);
    }

    public synchronized ConsumerGroup createGroup(String groupId){
        if (groups.containsKey(groupId)) {
            throw new IllegalArgumentException(
                    "Consumer group already exists: " + groupId
            );
        }
        ConsumerGroup group = new ConsumerGroup(groupId);

        groups.put(groupId,group);

        subscriptions.put(groupId, new HashMap<>());

        return group;
    }

    public synchronized ConsumerGroup getGroup(String groupId) {

        ConsumerGroup group = groups.get(groupId);

        if (group == null) {
            throw new IllegalArgumentException(
                    "Consumer group does not exist: " + groupId
            );
        }

        return group;
    }


    public synchronized void registerConsumer(
            String groupId,
            String consumerId
    ){
        ConsumerGroup group = getGroup(groupId);
        boolean added = group.addMember(consumerId);

        if(added){
            rebalance(groupId);
        }
    }

    public synchronized void removeConsumer(
            String groupId,
            String consumerId
    ) {

        ConsumerGroup group = getGroup(groupId);

        boolean removed = group.removeMember(consumerId);

        if (removed) {
            rebalance(groupId);
        }
    }

    public synchronized void subscribeTopic(
            String groupId,
            String topicName
    ) {
        ConsumerGroup group = getGroup(groupId);


        int partitionCount = topicManager.getPartitionCount(topicName);


        subscriptions.get(groupId).put(
                topicName,
                partitionCount
        );


        rebalance(groupId);
    }

    private synchronized void rebalance(String groupId){
        ConsumerGroup group = getGroup(groupId);

        Map<String,Integer> topics = subscriptions.get(groupId);

        for(Map.Entry<String,Integer> entry : topics.entrySet()){
            String topicName = entry.getKey();

            int partitionCount = entry.getValue();

            var assignments = assignor.assign(
                    group,
                    partitionCount
            );

            group.setAssignment(
                    topicName,
                    assignments
            );

            System.out.println(
                    "Rebalanced group '" + groupId
                            + "' for topic '" + topicName
                            + "': " + assignments
            );
        }

    }

    public synchronized Set<String> listGroups() {
        return Set.copyOf(groups.keySet());
    }


}
