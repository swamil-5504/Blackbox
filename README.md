# **BLACKBOX**

### **A Kafka-Inspired Messaging Broker Built from Scratch in Java**

BLACKBOX is a lightweight messaging broker developed from scratch using Java. Inspired by the core architecture of Apache Kafka, the project explores how modern messaging systems handle persistent storage, partitioned logs, producer-consumer communication, consumer group coordination, and broker-to-broker communication.

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

### **9. Multi-Broker Communication**

- Support for running multiple independent broker instances.
- Shared cluster configuration through `cluster.properties`.
- Unique broker IDs and separate client-facing and internal ports.
- Dedicated TCP servers for inter-broker communication.
- Broker identity and cluster name validation during handshakes.
- Prevention of self-connections and cross-cluster connections.
- Automatic connection retries when a peer is unavailable during startup.
- Persistent TCP connections between broker instances.
- PING/PONG request-response communication over established connections.
- Reusable peer connections maintained by the inter-broker client.

## **Architecture**

BLACKBOX follows a modular architecture, separating storage, messaging, networking, consumer coordination, and inter-broker communication responsibilities.

```text
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
├── network/
│   └── BlackboxServer.java
│
├── cluster/
│   ├── ClusterConfig.java
│   ├── InterBrokerServer.java
│   └── InterBrokerClient.java
│
└── resources/
    └── config/
        └── cluster.properties
```

### **Component Responsibilities**

| Component | Responsibility |
|---|---|
| `Message` | Immutable message representation |
| `PartitionLog` | Binary message storage and offset management |
| `TopicManager` | Topic creation, partition management, and recovery |
| `Producer` | Message publishing and partition selection |
| `Consumer` | Message fetching and group-aware consumption |
| `OffsetManager` | Persistent consumer group offset tracking |
| `ConsumerGroup` | Group membership and topic-specific partition assignments |
| `ConsumerGroupManager` | Group lifecycle, membership, subscriptions, and rebalancing |
| `PartitionAssignor` | Round-robin partition distribution |
| `BlackboxServer` | Client-facing TCP communication and request routing |
| `ClusterConfig` | Loads and validates broker cluster configuration |
| `InterBrokerServer` | Accepts and handles broker-to-broker connections |
| `InterBrokerClient` | Establishes and maintains connections to peer brokers |
| `Main` | Application initialization and dependency wiring |

## **System Architecture**

### **Client and Consumer Group Architecture**

```text
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

### **Multi-Broker Architecture**

BLACKBOX supports multiple broker processes running independently with separate storage directories and network ports.

```text
                 BLACKBOX CLUSTER
                 blackbox-local

       +----------------+  +----------------+
       |    Broker 1    |  |    Broker 2    |
       |                |  |                |
       | Client: 9092   |  | Client: 9093   |
       | Internal:19092 |  | Internal:19093 |
       +----------------+  +----------------+
                \                /
                 \              /
                  \            /
                   \          /
                 +----------------+
                 |    Broker 3    |
                 |                |
                 | Client: 9094   |
                 | Internal:19094 |
                 +----------------+
```

Each broker has:

- A unique broker ID.
- A client-facing port for producer and consumer requests.
- An internal port for inter-broker communication.
- An independent data directory.
- An independent consumer offset directory.

The internal communication layer uses persistent TCP connections to exchange requests between broker processes.

**Current limitation:** The brokers can establish and maintain inter-broker connections and exchange PING/PONG messages. Topic metadata synchronization, message replication, partition leadership, and failover are not yet implemented.

## **How Message Storage Works**

Each topic consists of one or more partitions. Every partition maintains its own independent message log and sequential offset.

For example, a topic named `orders` with three partitions has the following structure:

```text
data/
└── orders/
    ├── partition-0.log
    ├── partition-1.log
    └── partition-2.log
```

Example:

| Partition | Offset | Message |
|---|---|---|
| 0 | 0 | Order created |
| 0 | 1 | Payment received |
| 1 | 0 | Order dispatched |
| 2 | 0 | Order cancelled |

Offsets are independent across partitions.

Messages are appended to disk using binary serialization. When the broker restarts, the partition log reconstructs its next offset from the existing records.

## **Producer and Consumer**

### **Producer**

The producer publishes messages to a topic and selects a partition using round-robin distribution.

For a topic with three partitions, successive messages are distributed as follows:

```text
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

```text
payment-service
├── consumer-1
├── consumer-2
└── consumer-3
```

Different consumer groups can independently consume the same topic while maintaining separate committed offsets.

### **Partition Assignment**

BLACKBOX implements a round-robin partition assignment strategy.

Consider a topic with six partitions and a consumer group with three members:

```text
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

```text
consumer-1 → [0, 3]
consumer-2 → [1, 4]
consumer-3 → [2, 5]
```

**After removing consumer-2:**

```text
consumer-1 → [0, 2, 4]
consumer-3 → [1, 3, 5]
```

The updated assignments are available through the TCP API.

The current implementation does not yet include heartbeats or automatic detection of crashed or disconnected consumers.

## **Assignment-Aware Message Fetching**

BLACKBOX validates partition ownership before allowing a consumer to fetch messages through `FETCH_GROUP`.

For example:

```text
Topic: orders

consumer-1 → [0, 2]
consumer-2 → [1]
```

If `consumer-1` attempts to fetch partition `1`, the broker rejects the request.

Example error response:

```json
{
  "status": "ERROR",
  "message": "Consumer consumer-1 is not assigned partition 1"
}
```

This ensures that consumers within the same group only fetch messages from their assigned partitions.

## **Consumer Group Offset Management**

Each consumer group maintains independent progress for every topic partition.

Example directory:

```text
offsets/
├── payment-service.json
└── analytics-service.json
```

Example offset file:

```json
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

BLACKBOX exposes broker operations through a custom JSON-based TCP protocol on the client-facing port.

Requests are sent as newline-delimited JSON.

### **Supported Commands**

| Command | Description |
|---|---|
| `CREATE_TOPIC` | Creates a topic with a specified number of partitions |
| `PUBLISH` | Publishes a message to a topic |
| `FETCH` | Fetches messages from a specified partition and offset |
| `FETCH_GROUP` | Fetches messages using a consumer group's committed offset after validating partition ownership |
| `COMMIT_OFFSET` | Commits the next offset for a consumer group |
| `CREATE_GROUP` | Creates a consumer group |
| `JOIN_GROUP` | Adds a consumer to a group and triggers rebalancing |
| `LEAVE_GROUP` | Removes a consumer from a group and triggers rebalancing |
| `SUBSCRIBE` | Subscribes a group to a topic and triggers rebalancing |
| `GET_ASSIGNMENTS` | Retrieves a consumer's partition assignments for a topic |

### **Example Requests**

**1. Create a topic**

```json
{
  "command": "CREATE_TOPIC",
  "topic": "orders",
  "partitions": 3
}
```

**2. Publish a message**

```json
{
  "command": "PUBLISH",
  "topic": "orders",
  "key": "user-101",
  "value": "New order created"
}
```

Example response:

```json
{
  "partition": 0,
  "offset": 0,
  "status": "OK"
}
```

**3. Fetch messages**

```json
{
  "command": "FETCH",
  "topic": "orders",
  "partition": 0,
  "offset": 0,
  "limit": 10
}
```

**4. Create a consumer group**

```json
{
  "command": "CREATE_GROUP",
  "groupId": "payment-service"
}
```

**5. Subscribe a group to a topic**

```json
{
  "command": "SUBSCRIBE",
  "groupId": "payment-service",
  "topic": "orders"
}
```

**6. Join a consumer group**

```json
{
  "command": "JOIN_GROUP",
  "groupId": "payment-service",
  "consumerId": "consumer-1"
}
```

**7. Retrieve partition assignments**

```json
{
  "command": "GET_ASSIGNMENTS",
  "groupId": "payment-service",
  "consumerId": "consumer-1",
  "topic": "orders"
}
```

**8. Fetch using a consumer group**

```json
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

```json
{
  "command": "COMMIT_OFFSET",
  "groupId": "payment-service",
  "topic": "orders",
  "partition": 0,
  "offset": 5
}
```

**10. Leave a consumer group**

```json
{
  "command": "LEAVE_GROUP",
  "groupId": "payment-service",
  "consumerId": "consumer-1"
}
```

## **Multi-Broker Communication**

BLACKBOX supports running multiple independent broker instances using a shared cluster configuration.

### **Cluster Configuration**

Broker addresses and ports are defined in:

```text
src/main/resources/config/cluster.properties
```

Example configuration:

```properties
cluster.name=blackbox-local

broker-1.host=localhost
broker-1.port=9092
broker-1.internal-port=19092

broker-2.host=localhost
broker-2.port=9093
broker-2.internal-port=19093

broker-3.host=localhost
broker-3.port=9094
broker-3.internal-port=19094
```

`ClusterConfig` loads the configuration and validates broker addresses and port values.

### **Client and Internal Ports**

Each broker uses two separate ports:

| Port type | Purpose |
|---|---|
| Client-facing port | Receives producer and consumer requests |
| Internal port | Handles broker-to-broker communication |

For example:

| Broker | Client port | Internal port |
|---|---:|---:|
| Broker 1 | 9092 | 19092 |
| Broker 2 | 9093 | 19093 |
| Broker 3 | 9094 | 19094 |

Separating these ports keeps client-facing communication independent from internal broker communication.

### **Inter-Broker Handshake**

When two brokers connect, the initiating broker sends a handshake containing its broker ID and cluster name.

Example:

```text
HANDSHAKE|broker-1|blackbox-local
```

The receiving broker validates:

- The cluster name matches.
- The remote broker ID is not its own ID.
- The handshake follows the expected protocol format.

A successful handshake returns:

```text
HANDSHAKE_OK|broker-2|blackbox-local
```

Invalid handshakes, self-connections, and cluster mismatches are rejected.

### **Connection Retries**

If a peer broker is unavailable during startup, `InterBrokerClient` retries the connection.

The current configuration uses:

- Maximum attempts: 15
- Retry delay: 2 seconds
- TCP connection timeout: 3 seconds

This allows brokers to start at different times without immediately abandoning peer discovery.

### **Persistent Connections and PING/PONG**

After a successful handshake, the TCP connection remains open.

The server continuously reads requests from the same connection and currently supports the `PING` command.

Example request:

```text
PING
```

Example response:

```text
PONG|broker-2
```

The client validates the response and confirms successful communication with the peer.

Established peer connections are stored and reused by `InterBrokerClient`.

**Current limitations:**

- Automatic reconnection after an established connection breaks is not implemented.
- Broker metadata exchange is not implemented.
- Topic and partition metadata are not synchronized between brokers.
- Messages are not replicated between broker instances.
- Leader election and failover are not implemented.

The current multi-broker implementation establishes the networking foundation for future distributed messaging functionality.

## **Technology Stack**

| Technology | Purpose |
|---|---|
| Java 25 | Core implementation |
| Gradle | Build automation and dependency management |
| Java NIO | File-based storage |
| `FileChannel` | Binary log persistence |
| TCP Sockets | Client-server and inter-broker communication |
| Jackson | JSON serialization and deserialization |
| PowerShell | TCP API testing |

The project deliberately uses core Java to implement the broker's internal mechanisms instead of relying on an existing messaging framework.

## **Getting Started**

### **Prerequisites**

- JDK 25
- Gradle Wrapper (included in the repository)
- Git
- IntelliJ IDEA (recommended)

### **Clone the repository**

```bash
git clone <repository-url>
cd Blackbox
```

### **Build the project**

On Windows:

```powershell
.\gradlew.bat build
```

On Linux/macOS:

```bash
./gradlew build
```

### **Run a single broker**

Open the project in IntelliJ IDEA and run:

```text
org.blackbox.Main
```

Without startup arguments, the application uses the default broker ID, client port, data directory, and offset directory.

The default client-facing TCP server starts on port `9092`, provided the cluster configuration contains the corresponding broker entry.

### **Run Multiple Brokers**

BLACKBOX supports running multiple broker instances as separate Java processes.

Each instance accepts the following command-line arguments:

```text
<broker-id> <client-port> <data-directory> <offset-directory>
```

Example IntelliJ run configurations:

**Broker 1**

```text
broker-1 9092 data/broker-1 offsets/broker-1
```

**Broker 2**

```text
broker-2 9093 data/broker-2 offsets/broker-2
```

**Broker 3**

```text
broker-3 9094 data/broker-3 offsets/broker-3
```

Start the broker instances independently. Each broker loads the shared cluster configuration and attempts to establish connections with the other configured brokers.

Each instance maintains its own data and offset directories.

## **Runtime Storage**

BLACKBOX creates its runtime directories automatically.

Single-broker example:

```text
Blackbox/
├── data/
│   └── <topic>/
│       └── partition-<number>.log
│
└── offsets/
    └── <group-id>.json
```

Multi-broker example:

```text
Blackbox/
├── data/
│   ├── broker-1/
│   ├── broker-2/
│   └── broker-3/
│
└── offsets/
    ├── broker-1/
    ├── broker-2/
    └── broker-3/
```

- `data/` stores persistent partition logs.
- `offsets/` stores consumer group progress.
- Each broker uses its own configured directories.

These directories are runtime-generated and excluded from version control through `.gitignore`.

Deleting these directories resets the corresponding local broker data and consumer progress.

## **Current Implementation Status**

| Feature | Status |
|---|---|
| Immutable message model | Implemented |
| Binary append-only partition logs | Implemented |
| Sequential partition offsets | Implemented |
| Topic creation | Implemented |
| Multiple partitions per topic | Implemented |
| Producer with round-robin routing | Implemented |
| Offset-based consumer fetching | Implemented |
| Topic recovery on startup | Implemented |
| Persistent consumer group offsets | Implemented |
| Group-aware message fetching | Implemented |
| Offset commit validation | Implemented |
| TCP server and JSON protocol | Implemented |
| Consumer group creation and management | Implemented |
| Consumer registration and removal | Implemented |
| Topic subscriptions | Implemented |
| Round-robin partition assignment | Implemented |
| Automatic rebalancing on membership changes | Implemented |
| Assignment-aware group fetching | Implemented |
| TCP integration for consumer coordination | Implemented |
| Multiple independent broker instances | Implemented |
| Shared cluster configuration | Implemented |
| Inter-broker handshake and validation | Implemented |
| Startup connection retries | Implemented |
| Persistent inter-broker TCP connections | Implemented |
| PING/PONG communication | Implemented |
| Consumer heartbeats and failure detection | Planned |
| Automatic reconnection after disconnection | Planned |
| Broker metadata exchange | Planned |
| Log segmentation and indexing | Planned |
| Message replication | Planned |
| Leader-follower architecture | Planned |
| Replication consistency | Planned |
| Leader election and failover | Planned |
| Stream processing | Planned |


The long-term objective is to build a lightweight, reliable, and distributed event streaming system while understanding the engineering trade-offs behind each component.
