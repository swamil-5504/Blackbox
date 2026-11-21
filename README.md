# **BLACKBOX**

### **A Kafka-Inspired Messaging Broker Built from Scratch in Java**

BLACKBOX is a lightweight messaging broker developed from scratch using Java. Inspired by the core architecture of Apache Kafka, the project explores how modern messaging systems handle persistent storage, partitioned logs, producer-consumer communication, and consumer group coordination.

Rather than relying on existing messaging frameworks, BLACKBOX implements its core mechanisms using Java, Java NIO, TCP sockets, and Gradle.

The objective is to understand the engineering principles behind reliable messaging systems and progressively build a more scalable and distributed event streaming platform.

## **Key Features**

### **1. Persistent Message Storage**

- Append-only binary partition logs.
- Disk-based message persistence using Java NIO `FileChannel`.
- Sequential offset assignment within each partition.
- Automatic offset recovery when reopening existing partition logs.

### **2. Topic and Partition Management**

- Dynamic topic creation.
- Configurable partition count per topic.
- Independent log files for each partition.
- Automatic discovery and recovery of existing topics during startup.
- Partition numbering validation during recovery.

### **3. Producer**

- Dedicated producer component for publishing messages.
- Round-robin partition selection.
- Thread-safe partition selection using `AtomicInteger`.
- Returns the assigned partition and message offset after publishing.

### **4. Consumer**

- Offset-based message consumption.
- Configurable fetch limits.
- Partition-specific message retrieval.
- Consumer group-aware polling using committed offsets.
- Explicit offset commits.

### **5. Consumer Group Offset Management**

- Independent offsets for different consumer groups.
- Persistent offset storage using JSON.
- Automatic retrieval of previously committed offsets.
- Offset validation before committing.
- Resume consumption from the previously committed position.

### **6. Consumer Group Coordination**

- Consumer group creation and management.
- Consumer registration and removal.
- Tracking of active group members.
- Duplicate consumer registration prevention.
- Round-robin partition assignment among group members.
- Topic-specific partition assignment storage.
- Automatic partition rebalancing when consumers join or leave.
- Rebalancing when group topic subscriptions change.
- Retrieval of assignments for individual consumers.

### **7. Assignment-Aware Message Fetching**

- Validates consumer group membership before fetching.
- Verifies that the group is subscribed to the requested topic.
- Ensures consumers can only fetch partitions assigned to them.
- Prevents consumers from fetching partitions assigned to other group members.

### **8. TCP-Based Client-Server Communication**

- TCP server implemented using Java sockets.
- JSON-based request and response protocol.
- Newline-delimited request processing.
- Multiple client connections handled using separate threads.
- Structured responses for invalid requests.
- TCP integration for producer, consumer, and consumer group operations.

## **Architecture**

BLACKBOX follows a modular architecture, separating storage, messaging, networking, and consumer coordination responsibilities.

```
BLACKBOX
│
├── Main.java
│
├── message/
│   └── Message.java
│
├── storage/
│   └── PartitionLog.java
│
├── topic/
│   └── TopicManager.java
│
├── producer/
│   └── Producer.java
│
├── consumer/
│   ├── Consumer.java
│   ├── OffsetManager.java
│   ├── ConsumerGroup.java
│   ├── ConsumerGroupManager.java
│   └── PartitionAssignor.java
│
└── network/
    └── BlackboxServer.java
```

### **Component Responsibilities**

| Component              | Responsibility                                              |
| ---------------------- | ----------------------------------------------------------- |
| `Message`              | Immutable message representation                            |
| `PartitionLog`         | Binary message storage and offset management                |
| `TopicManager`         | Topic creation, partition management, and recovery          |
| `Producer`             | Message publishing and partition selection                  |
| `Consumer`             | Message fetching and group-aware consumption                |
| `OffsetManager`        | Persistent consumer group offset tracking                   |
| `ConsumerGroup`        | Group membership and topic-specific partition assignments   |
| `ConsumerGroupManager` | Group lifecycle, membership, subscriptions, and rebalancing |
| `PartitionAssignor`    | Round-robin partition distribution                          |
| `BlackboxServer`       | TCP communication and request routing                       |
| `Main`                 | Application initialization and dependency wiring            |

## **System Architecture**

```
                 Client Applications
                         |
                         | TCP / JSON
                         |
                   BlackboxServer
                         |
              -----------------------
              |                     |
           Producer               Consumer
              |                     |
              |                OffsetManager
              |                     |
              |                     |
              ------ TopicManager
                         |
                    PartitionLog
                         |
                  Persistent Storage


              Consumer Coordination

                 ConsumerGroupManager
                         |
                    ConsumerGroup
                         |
                   PartitionAssignor
                         |
                 Partition Assignments
```

Messages are stored in partition-specific append-only logs, while consumer group progress is maintained separately through persistent offset files.

Consumer group membership and partition assignments are managed through dedicated Java components. When a consumer joins or leaves a group, or when the group subscribes to a topic, BLACKBOX recalculates partition assignments. These operations are exposed through the TCP protocol.

**Persistence distinction:** Consumer group offsets are persisted to disk. Consumer group membership and partition assignments currently exist in memory and must be recreated through the TCP API after a broker restart.

## **How Message Storage Works**

Each topic consists of one or more partitions. Every partition maintains its own independent message log and sequential offset.

For example, a topic named `orders` with three partitions has the following structure:

```
data/
└── orders/
    ├── partition-0.log
    ├── partition-1.log
    └── partition-2.log
```

Example:

| Partition | Offset | Message          |
| --------- | ------ | ---------------- |
| 0         | 0      | Order created    |
| 0         | 1      | Payment received |
| 1         | 0      | Order dispatched |
| 2         | 0      | Order cancelled  |

Offsets are independent across partitions.

Messages are appended to disk using binary serialization. When the broker restarts, the partition log reconstructs its next offset from the existing records.

## **Producer and Consumer**

### **Producer**

The producer publishes messages to a topic and selects a partition using round-robin distribution.

For a topic with three partitions, successive messages are distributed as follows:

```
Message 1 → Partition 0
Message 2 → Partition 1
Message 3 → Partition 2
Message 4 → Partition 0
Message 5 → Partition 1
Message 6 → Partition 2
```

The producer returns the partition number and offset assigned to each message.

### **Consumer**

The consumer retrieves messages from a specific topic partition using an offset and a maximum fetch limit.

It also supports group-aware polling, which retrieves messages starting from a consumer group's committed offset.

## **Consumer Groups and Partition Assignment**

Consumer groups allow multiple consumers to coordinate the consumption of messages.

Each group has a unique group ID and maintains its own membership.

For example:

```
payment-service
├── consumer-1
├── consumer-2
└── consumer-3
```

Different consumer groups can independently consume the same topic while maintaining separate committed offsets.

### **Partition Assignment**

BLACKBOX implements a round-robin partition assignment strategy.

Consider a topic with six partitions and a consumer group with three members:

```
Topic: orders
Partitions: 0, 1, 2, 3, 4, 5

consumer-1 → [0, 3]
consumer-2 → [1, 4]
consumer-3 → [2, 5]
```

Partitions are distributed cyclically across available group members.

The `PartitionAssignor` calculates these assignments, while `ConsumerGroup` stores assignments separately for each topic.

`ConsumerGroupManager` coordinates membership and triggers rebalancing when members join or leave, or when a topic subscription changes.

### **Automatic Rebalancing**

When a consumer joins or leaves a group, BLACKBOX recalculates partition assignments among the remaining active members.

For example:

**Before:**

```
consumer-1 → [0, 3]
consumer-2 → [1, 4]
consumer-3 → [2, 5]
```

**After removing consumer-2:**

```
consumer-1 → [0, 2, 4]
consumer-3 → [1, 3, 5]
```

The updated assignments are available through the TCP API.

The current implementation does not yet include heartbeats or automatic detection of crashed or disconnected consumers.

## **Assignment-Aware Message Fetching**

BLACKBOX validates partition ownership before allowing a consumer to fetch messages through `FETCH_GROUP`.

For example:

```
Topic: orders

consumer-1 → [0, 2]
consumer-2 → [1]
```

If `consumer-1` attempts to fetch partition `1`, the broker rejects the request.

Example error response:

```
{
  "status": "ERROR",
  "message": "Consumer consumer-1 is not assigned partition 1"
}
```

This ensures that consumers within the same group only fetch messages from their assigned partitions.

## **Consumer Group Offset Management**

Each consumer group maintains independent progress for every topic partition.

Example directory:

```
offsets/
├── payment-service.json
└── analytics-service.json
```

Example offset file:

```
{
  "orders": {
    "0": 4,
    "1": 5,
    "2": 2
  },
  "payments": {
    "0": 4
  }
}
```

Offsets represent the **next message to be consumed**, rather than the last message processed.

For example:

1. A consumer reads the message at offset `3`.
2. After processing it, the consumer commits offset `4`.
3. The next fetch starts from offset `4`.
4. After restarting, the consumer can resume from its saved position.

This provides independent consumer progress and basic message processing recovery.

## **TCP API**

BLACKBOX exposes broker operations through a custom JSON-based TCP protocol on port `9092`.

Requests are sent as newline-delimited JSON.

### **Supported Commands**

| Command           | Description                                                                                     |
| ----------------- | ----------------------------------------------------------------------------------------------- |
| `CREATE_TOPIC`    | Creates a topic with a specified number of partitions                                           |
| `PUBLISH`         | Publishes a message to a topic                                                                  |
| `FETCH`           | Fetches messages from a specified partition and offset                                          |
| `FETCH_GROUP`     | Fetches messages using a consumer group's committed offset after validating partition ownership |
| `COMMIT_OFFSET`   | Commits the next offset for a consumer group                                                    |
| `CREATE_GROUP`    | Creates a consumer group                                                                        |
| `JOIN_GROUP`      | Adds a consumer to a group and triggers rebalancing                                             |
| `LEAVE_GROUP`     | Removes a consumer from a group and triggers rebalancing                                        |
| `SUBSCRIBE`       | Subscribes a group to a topic and triggers rebalancing                                          |
| `GET_ASSIGNMENTS` | Retrieves a consumer's partition assignments for a topic                                        |

### **Example Requests**

**1. Create a topic**

```
{
  "command": "CREATE_TOPIC",
  "topic": "orders",
  "partitions": 3
}
```

**2. Publish a message**

```
{
  "command": "PUBLISH",
  "topic": "orders",
  "key": "user-101",
  "value": "New order created"
}
```

Example response:

```
{
  "partition": 0,
  "offset": 0,
  "status": "OK"
}
```

**3. Fetch messages**

```
{
  "command": "FETCH",
  "topic": "orders",
  "partition": 0,
  "offset": 0,
  "limit": 10
}
```

**4. Create a consumer group**

```
{
  "command": "CREATE_GROUP",
  "groupId": "payment-service"
}
```

**5. Subscribe a group to a topic**

```
{
  "command": "SUBSCRIBE",
  "groupId": "payment-service",
  "topic": "orders"
}
```

**6. Join a consumer group**

```
{
  "command": "JOIN_GROUP",
  "groupId": "payment-service",
  "consumerId": "consumer-1"
}
```

**7. Retrieve partition assignments**

```
{
  "command": "GET_ASSIGNMENTS",
  "groupId": "payment-service",
  "consumerId": "consumer-1",
  "topic": "orders"
}
```

**8. Fetch using a consumer group**

```
{
  "command": "FETCH_GROUP",
  "groupId": "payment-service",
  "consumerId": "consumer-1",
  "topic": "orders",
  "partition": 0,
  "limit": 10
}
```

**9. Commit a consumer group offset**

```
{
  "command": "COMMIT_OFFSET",
  "groupId": "payment-service",
  "topic": "orders",
  "partition": 0,
  "offset": 5
}
```

**10. Leave a consumer group**

```
{
  "command": "LEAVE_GROUP",
  "groupId": "payment-service",
  "consumerId": "consumer-1"
}
```

## **Technology Stack**

| Technology    | Purpose                                    |
| ------------- | ------------------------------------------ |
| Java 25       | Core implementation                        |
| Gradle        | Build automation and dependency management |
| Java NIO      | File-based storage                         |
| `FileChannel` | Binary log persistence                     |
| TCP Sockets   | Client-server communication                |
| Jackson       | JSON serialization and deserialization     |
| PowerShell    | TCP API testing                            |

The project deliberately uses core Java to implement the broker's internal mechanisms instead of relying on an existing messaging framework.

## **Getting Started**

### **Prerequisites**

- JDK 25
- Gradle Wrapper (included in the repository)
- Git
- IntelliJ IDEA (recommended)

### **Clone the repository**

```
git clone <repository-url>
cd Blackbox
```

### **Build the project**

On Windows:

```
.\gradlew.bat build
```

On Linux/macOS:

```
./gradlew build
```

### **Run the application**

Open the project in IntelliJ IDEA and run:

```
org.blackbox.Main
```

The TCP server starts on port `9092`.

Expected startup output:

```
BLACKBOX server listening on port 9092
```

## **Runtime Storage**

BLACKBOX creates its runtime directories automatically.

```
Blackbox/
├── data/
│   └── <topic>/
│       └── partition-<number>.log
│
└── offsets/
    └── <group-id>.json
```

- `data/` stores persistent partition logs.
- `offsets/` stores consumer group progress.

These directories are runtime-generated and excluded from version control through `.gitignore`.

Deleting these directories resets local broker data and consumer progress.

## **Current Implementation Status**

| Feature                                     | Status      |
| ------------------------------------------- | ----------- |
| Immutable message model                     | Implemented |
| Binary append-only partition logs           | Implemented |
| Sequential partition offsets                | Implemented |
| Topic creation                              | Implemented |
| Multiple partitions per topic               | Implemented |
| Producer with round-robin routing           | Implemented |
| Offset-based consumer fetching              | Implemented |
| Topic recovery on startup                   | Implemented |
| Persistent consumer group offsets           | Implemented |
| Group-aware message fetching                | Implemented |
| Offset commit validation                    | Implemented |
| TCP server and JSON protocol                | Implemented |
| Consumer group creation and management      | Implemented |
| Consumer registration and removal           | Implemented |
| Topic subscriptions                         | Implemented |
| Round-robin partition assignment            | Implemented |
| Automatic rebalancing on membership changes | Implemented |
| Assignment-aware group fetching             | Implemented |
| TCP integration for consumer coordination   | Implemented |
| Consumer heartbeats and failure detection   | Planned     |
| Log segmentation and indexing               | Planned     |
| Replication and multi-broker clustering     | Planned     |
| Stream processing                           | Planned     |

## **Roadmap**

BLACKBOX is under active development. The next milestones focus on evolving the broker into a more complete distributed messaging and event streaming platform.

### **Phase 1 – Consumer Group Coordination**

- Consumer group creation and management. *(Implemented)*
- Consumer registration and removal. *(Implemented)*
- Topic subscriptions. *(Implemented)*
- Round-robin partition assignment and rebalancing on membership or subscription changes. *(Implemented)*
- Assignment-aware group fetching through the TCP API. *(Implemented)*
- Consumer heartbeats and automatic failure detection. *(Planned)*

### **Phase 2 – Storage Engine Improvements**

- Indexed partition reads.
- Log segmentation.
- Atomic offset persistence.
- Improved crash recovery.
- Message retention and cleanup.

### **Phase 3 – Reliability**

- At-least-once delivery semantics.
- Retry mechanisms.
- Dead-letter queues.
- Idempotent message publishing.
- Improved failure handling.

### **Phase 4 – Distributed Architecture**

- Multiple broker instances.
- Partition replication.
- Leader-follower architecture.
- Broker failure detection.
- Replication consistency.
- Leader election and failover.

### **Phase 5 – Stream Processing**

- Event filtering and transformation.
- Window-based aggregations.
- Stateful event processing.
- Real-time event pipelines.

### **Phase 6 – Production Engineering**

- Performance benchmarking.
- Monitoring and metrics.
- Integration and stress testing.
- Docker-based deployment.
- Operational documentation.

## **Engineering Goals**

BLACKBOX is a hands-on exploration of:

- Message broker architecture.
- Distributed systems fundamentals.
- Persistent storage engines.
- Concurrent programming in Java.
- TCP networking and client-server communication.
- Producer-consumer patterns.
- Event-driven architecture.
- Consumer group coordination.
- Partition assignment strategies.
- Fault tolerance and message delivery guarantees.

The long-term objective is to build a lightweight, reliable, and distributed event streaming system while understanding the engineering trade-offs behind each component.

**BLACKBOX — Building the fundamentals of distributed messaging, one component at a time.**
