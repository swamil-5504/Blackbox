package org.blackbox.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConsumerGroupManager {

    private final Map<String, ConsumerGroup> groups =
            new HashMap<>();

    public synchronized ConsumerGroup createGroup(String groupId){
        if(groups.containsKey(groupId)){
            throw new IllegalArgumentException(
                    "Consumer group already exists: " + groupId
            );
        }

        ConsumerGroup group = new ConsumerGroup(groupId);

        groups.put(groupId,group);

        return group;
    }

    public synchronized ConsumerGroup getGroup(String groupId){
        ConsumerGroup group = groups.get(groupId);

        if(group == null){
            throw new IllegalArgumentException(
                    "Consumer group does not exist " + groupId
            );
        }
        return group;
    }

    public void registerConsumer(
            String groupId,
            String consumerId
    ) {
        ConsumerGroup group = getGroup(groupId);

        group.addMember(consumerId);
    }

    public void removeConsumer(
            String groupId,
            String consumerId
    ) {
        ConsumerGroup group = getGroup(groupId);

        group.removeMember(consumerId);
    }

    public synchronized Set<String> listGroups(){
        return Set.copyOf(groups.keySet());
    }

}
