# BLACKBOX

### A Lightweight Distributed Messaging Broker Built from Scratch in Java

BLACKBOX is a Java-based message broker inspired by the architecture and core concepts of Apache Kafka. It is designed to explore the engineering principles behind modern messaging systems, including persistent message storage, partitioned logs, producer-consumer communication, and consumer group offset management.

The project is being developed from scratch using core Java, without relying on messaging frameworks or existing broker implementations.

The goal is to understand and implement the fundamental building blocks of a reliable, scalable messaging system.

---

## Key Features

### 1. Persistent Message Storage
- Append-only binary partition logs.
- Disk-based message persistence using Java NIO `FileChannel`.
- Sequential offset assignment within each partition.
- Automatic offset recovery when reopening existing partition logs.

### 2. Topic and Partition Management
- Dynamic topic creation.
- Configurable partition count per topic.
- Independent log files for each partition.
- Automatic discovery and recovery of existing topics during startup.
- Partition filename validation and recovery checks.

### 3. Producer
- Message publishing through a dedicated producer component.
- Round-robin partition selection.
- Thread-safe partition selection using `AtomicInteger`.
- Returns the assigned partition and message offset after publishing.

### 4. Consumer
- Offset-based message consumption.
- Configurable fetch limits.
- Partition-specific message retrieval.
- Consumer group-aware polling using committed offsets.

### 5. Consumer Group Offset Management
- Independent offsets for different consumer groups.
- Persistent offset storage using JSON.
- Automatic retrieval of the last committed offset.
- Offset validation before committing.
- Resume consumption from the previously committed position.

### 6. TCP-Based Client-Server Communication
- TCP server built using Java sockets.
- JSON-based request and response protocol.
- Newline-delimited request processing.
- Multiple client connections handled using threads.
- Structured error responses for invalid requests.

---

## Architecture

BLACKBOX follows a modular architecture in which each component has a clearly defined responsibility.

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
│   └── OffsetManager.java
│
└── network/
    └── BlackboxServer.java
```

### Component Responsibilities

| Component | Responsibility |
|---|---|
| `Message` | Immutable message representation |
| `PartitionLog` | Binary message storage and offset management |
| `TopicManager` | Topic creation, partition management and recovery |
| `Producer` | Message publishing and partition selection |
| `Consumer` | Message fetching and group-aware consumption |
| `OffsetManager` | Persistent consumer group offset tracking |
| `BlackboxServer` | TCP communication and request routing |
| `Main` | Application initialization and dependency wiring |

---

## System Architecture

```text
              Client Applications
                      |
                      | TCP / JSON
                      |
               BlackboxServer
                      |
             -------------------
             |                 |
          Producer          Consumer
             |                 |
             |            OffsetManager
             |                 |
             |                 |
             ------ TopicManager
                      |
                 PartitionLog
                      |
              Persistent Storage
```

Messages are stored in partition-specific append-only logs, while consumer group progress is maintained separately through persistent offset files.

This separation allows multiple consumer groups to independently consume the same underlying message data.

---

## How Message Storage Works

Each topic consists of one or more partitions.

For example, a topic named `orders` with three partitions has the following structure:

```text
data/
└── orders/
    ├── partition-0.log
    ├── partition-1.log
    └── partition-2.log
```

Each partition maintains its own sequential offset.

Example:

| Partition | Offset | Message |
|---|---:|---|
| 0 | 0 | Order created |
| 0 | 1 | Payment received |
| 1 | 0 | Order dispatched |
| 2 | 0 | Order cancelled |

Offsets are independent across partitions.

Messages are appended to disk using binary serialization, and the partition log reconstructs its next offset during recovery.

---

## Consumer Groups and Offset Management

BLACKBOX supports basic consumer group offset tracking.

Each consumer group maintains independent progress for every topic partition.

For example:

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

This provides the foundation for independent consumer progress and basic message processing recovery.

---

## TCP API

BLACKBOX exposes its broker operations through a custom JSON-based TCP protocol on port `9092`.

Requests are sent as newline-delimited JSON.

### Supported Commands

| Command | Description |
|---|---|
| `CREATE_TOPIC` | Creates a topic with a specified number of partitions |
| `PUBLISH` | Publishes a message to a topic |
| `FETCH` | Fetches messages from a specified partition and offset |
| `FETCH_GROUP` | Fetches messages using a consumer group's committed offset |
| `COMMIT_OFFSET` | Commits the next offset for a consumer group |

### Example Requests

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

**4. Fetch using a consumer group**

```json
{
  "command": "FETCH_GROUP",
  "groupId": "payment-service",
  "topic": "orders",
  "partition": 0,
  "limit": 10
}
```

**5. Commit a consumer group offset**

```json
{
  "command": "COMMIT_OFFSET",
  "groupId": "payment-service",
  "topic": "orders",
  "partition": 0,
  "offset": 5
}
```

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Java 25 | Core implementation |
| Gradle | Build automation and dependency management |
| Java NIO | File-based storage |
| `FileChannel` | Binary log persistence |
| TCP Sockets | Client-server communication |
| Jackson | JSON serialization and deserialization |
| PowerShell | TCP API testing |

The project deliberately uses core Java to implement the broker's internal mechanisms rather than relying on an existing messaging framework.

---

## Getting Started

### Prerequisites

- JDK 25
- Gradle Wrapper (included in the repository)
- Git

### Clone the repository

```bash
git clone <repository-url>
cd Blackbox
```

### Build the project

On Windows:

```powershell
.\gradlew.bat build
```

On Linux/macOS:

```bash
./gradlew build
```

### Run the application

Open the project in IntelliJ IDEA and run:

```text
org.blackbox.Main
```

The server starts on port `9092`.

Expected startup output:

```text
BLACKBOX server listening on port 9092
```

---

## Runtime Storage

BLACKBOX creates its runtime directories automatically.

```text
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

---

## Current Implementation Status

| Feature | Status |
|---|---|
| Message model | Implemented |
| Binary append-only partition logs | Implemented |
| Sequential partition offsets | Implemented |
| Topic creation | Implemented |
| Multiple partitions per topic | Implemented |
| Producer with round-robin routing | Implemented |
| Offset-based consumer fetching | Implemented |
| Topic recovery on startup | Basic implementation |
| Persistent consumer group offsets | Implemented |
| Group-aware message fetching | Implemented |
| Offset commit validation | Implemented |
| TCP server and JSON protocol | Implemented |
| Consumer membership and coordination | Planned |
| Automatic partition assignment | Planned |
| Consumer rebalancing | Planned |
| Log segmentation and indexing | Planned |
| Replication and multi-broker clustering | Planned |
| Stream processing | Planned |

---

## Roadmap

BLACKBOX is under active development. The next milestones focus on moving from a basic message broker toward a more complete distributed messaging and streaming platform.

### Phase 1 – Consumer Group Coordination
- Consumer registration and membership.
- Multiple consumers within a group.
- Automatic partition assignment.
- Partition rebalancing.
- Consumer heartbeats and failure detection.

### Phase 2 – Storage Engine Improvements
- Indexed partition reads.
- Log segmentation.
- Atomic offset persistence.
- Improved crash recovery.
- Message retention and cleanup.

### Phase 3 – Reliability
- At-least-once delivery semantics.
- Retry mechanisms.
- Dead-letter queues.
- Idempotent message publishing.
- Improved failure handling.

### Phase 4 – Distributed Architecture
- Multiple broker instances.
- Partition replication.
- Leader-follower architecture.
- Broker failure detection.
- Replication consistency.

### Phase 5 – Stream Processing
- Event filtering and transformation.
- Window-based aggregations.
- Stateful event processing.
- Real-time event pipelines.

### Phase 6 – Production Engineering
- Performance benchmarking.
- Monitoring and metrics.
- Integration and stress testing.
- Docker-based deployment.
- Operational documentation.

---

## Engineering Goals

BLACKBOX is being developed as a hands-on exploration of:

- Message broker architecture.
- Distributed systems fundamentals.
- Persistent storage engines.
- Concurrent programming in Java.
- TCP networking and client-server communication.
- Producer-consumer patterns.
- Event-driven architecture.
- Consumer group coordination.
- Fault tolerance and message delivery guarantees.

The long-term objective is to build a lightweight, reliable and distributed event streaming system while understanding the engineering trade-offs behind every component.

---

**BLACKBOX — Building the fundamentals of distributed messaging, one component at a time.**
