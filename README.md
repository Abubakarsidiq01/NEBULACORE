# NebulaCore

A distributed event streaming platform inspired by Apache Kafka, built from scratch in C++ with a focus on teaching real distributed systems engineering through hands-on implementation.

## Overview

NebulaCore is a production-quality distributed messaging system that implements core distributed systems primitives including consensus algorithms, replication, fault tolerance, and network resilience. It provides a Kafka-like publish-subscribe model with topics, partitions, and consumer groups.

## Key Features

- **Raft Consensus Algorithm**: Full implementation of the Raft consensus protocol for leader election and log replication
- **Custom Write-Ahead Log (WAL)**: High-performance segmented log storage with retention policies
- **Gossip Protocol**: Cluster membership and leader discovery using gossip-based communication
- **Multi-Partition Topics**: Key-based partitioning for horizontal scalability
- **Consumer Groups**: Offset tracking and management for multiple consumer groups
- **Fault Tolerance**: Automatic leader election, failover, and network retry logic
- **Binary Protocol**: Efficient length-prefixed binary protocol for client communication
- **Python Client Library**: Full-featured Python client with automatic leader discovery and failover

## Architecture

### Core Components

- **NebulaNode**: Main node implementation that orchestrates all subsystems
- **RaftNode**: Raft consensus implementation for distributed coordination
- **TopicManager**: Manages topics, partitions, and consumer group offsets
- **NebulaLog**: Segmented write-ahead log with retention and recovery
- **GossipCluster**: Cluster membership and discovery
- **LeaderElector**: Leader election using gossip protocol
- **NodeServer**: TCP server for client connections
- **RaftServer**: Dedicated server for Raft RPCs (AppendEntries, RequestVote)

### Network Ports

Each node uses three ports:
- **Gossip Port**: Cluster membership and leader election (e.g., 5001)
- **Raft Port**: Gossip port + 1000 (e.g., 6001) for Raft RPCs
- **Client Port**: Gossip port + 2000 (e.g., 7001) for client API

## Building

### Prerequisites

- C++17 compatible compiler (GCC, Clang, or MSVC)
- CMake 3.15 or higher
- Boost libraries (system component)
- Python 3.x (for Python client)

### Build Instructions

```bash
cd nebula_core
mkdir build && cd build
cmake ..
make
```

On Windows with Visual Studio:
```bash
cd nebula_core
mkdir build && cd build
cmake .. -G "Visual Studio 17 2022"
cmake --build . --config Release
```

## Running a Cluster

### Start Three Nodes

**Terminal 1 (Node n1):**
```bash
./nebula_node_main n1 127.0.0.1 5001 n2:127.0.0.1:5002 n3:127.0.0.1:5003
```

**Terminal 2 (Node n2):**
```bash
./nebula_node_main n2 127.0.0.1 5002 n1:127.0.0.1:5001 n3:127.0.0.1:5003
```

**Terminal 3 (Node n3):**
```bash
./nebula_node_main n3 127.0.0.1 5003 n1:127.0.0.1:5001 n2:127.0.0.1:5002
```

### Using the Python Client

```python
from nebula_client import NebulaClient

# Connect to any node (client will discover leader automatically)
client = NebulaClient("127.0.0.1", 7001, seeds=[
    ("127.0.0.1", 7001),
    ("127.0.0.1", 7002),
    ("127.0.0.1", 7003)
])

# Publish a message
result = client.publish("alerts", "key1", "Hello, NebulaCore!")
print(f"Published to partition {result['partition']} at offset {result['offset']}")

# Consume messages (using the binary protocol directly)
# See python_client/consume.py for examples
```

## Example Scripts

The `python_client/` directory contains example scripts:

- **publish.py**: Simple message publisher
- **consume.py**: Message consumer example
- **bench_publish.py**: Performance benchmarking tool
- **failover_test.py**: Tests leader failover scenarios
- **leader.py**: Query current cluster leader

## Technical Details

### Raft Implementation

- Full Raft consensus protocol with leader election and log replication
- Handles network partitions and node failures
- Automatic leader redirect for client requests
- Promise-based async publish operations that wait for Raft commit

### Log Storage

- Segmented log files with configurable segment size (default 8MB)
- Retention policies: max segments and max bytes
- Efficient offset indexing for fast reads
- Automatic log recovery on restart
- Safe cleanup that respects consumer group offsets

### Replication

- Leader replicates all writes to followers via Raft
- Majority quorum required for commits
- Automatic retry logic for network failures
- Handles follower lag and catch-up

### Consumer Groups

- Per-group offset tracking across all partitions
- Offset persistence to disk
- Automatic retention based on minimum consumer offset
- Support for multiple consumer groups per topic

## Testing

The project includes comprehensive test suites:

```bash
# Run all tests
cd build
make test

# Individual test examples:
./test_raft_phase1    # Basic Raft functionality
./test_raft_phase2    # Leader election
./test_raft_phase3    # Log replication
./test_node           # Multi-node cluster
./test_topics         # Topic and partition management
./test_nebula_log     # Log storage and recovery
```

## Project Structure

```
nebula_core/
├── src/              # Core implementation
│   ├── raft.cpp/h    # Raft consensus algorithm
│   ├── nebula_node.cpp/h  # Main node implementation
│   ├── topic_manager.cpp/h  # Topic and partition management
│   ├── nebula_log.cpp/h     # Write-ahead log engine
│   ├── cluster.cpp/h       # Gossip cluster membership
│   ├── leader.cpp/h         # Leader election
│   ├── node_server.cpp/h   # Client TCP server
│   └── raft_server.cpp/h   # Raft RPC server
├── tests/            # Test suites
├── python_client/    # Python client library and examples
└── CMakeLists.txt    # Build configuration
```

## Design Philosophy

NebulaCore is designed as both a learning project and a production-oriented implementation. It demonstrates:

- **Consensus Algorithms**: How Raft ensures consistency across distributed nodes
- **Replication**: Leader-follower replication with majority quorums
- **Fault Tolerance**: Handling node failures, network partitions, and leader changes
- **Performance**: Efficient binary protocols, segmented logs, and offset indexing
- **Resilience**: Automatic retry, failover, and recovery mechanisms

## License

This project is designed for educational purposes and demonstrates distributed systems concepts through practical implementation.

## Contributing

This is an educational project showcasing distributed systems engineering. Contributions that improve correctness, performance, or documentation are welcome.

---

**Built with C++17, Boost.Asio, and a passion for distributed systems.**
