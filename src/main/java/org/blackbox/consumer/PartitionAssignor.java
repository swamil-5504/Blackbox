
package org.blackbox.consumer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PartitionAssignor {

    public Map<String, List<Integer>> assign(
            ConsumerGroup group,
            int partitionCount
    ) {

        if (group == null) {
            throw new IllegalArgumentException(
                    "Consumer group cannot be null"
            );
        }

        if (partitionCount <= 0) {
            throw new IllegalArgumentException(
                    "Partition count must be greater than zero"
            );
        }


        List<String> members = group.getMembers();


        if (members.isEmpty()) {
            return Map.of();
        }


        Map<String, List<Integer>> assignments =
                new LinkedHashMap<>();

        for (String member : members) {
            assignments.put(member, new ArrayList<>());
        }

        for(int partition = 0;
            partition < partitionCount;
            partition++){
            int consumerIndex = partition % members.size();
            String consumerId = members.get(consumerIndex);

            assignments.get(consumerId).add(partition);
        }

        return assignments;
    }
}
