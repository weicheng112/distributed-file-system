# Distributed File System with Protocol Buffers

A distributed file system (DFS) implemented in Go using Protocol Buffers for serialization. The system consists of three main components: Controller, Storage Nodes, and Client.

## Overview

This distributed file system is designed to provide reliable, scalable file storage across multiple nodes. It follows a controller-storage node architecture similar to HDFS, with added features for replication, fault tolerance, and parallel operations.

## Components

### Controller

The Controller acts as the central coordinator (similar to HDFS NameNode), managing metadata about files, chunks, and storage nodes. It:
- Maintains information about active storage nodes
- Tracks file-to-chunk mappings and their locations
- Handles storage node registration and heartbeats
- Manages chunk placement and replication
- Detects node failures and ensures replication levels

The Controller itself never sees any of the actual files, only their metadata.

### Storage Nodes

Storage nodes are responsible for storing and retrieving file chunks. Each node:
- Stores chunks with checksums for data integrity
- Sends regular heartbeats to the controller
- Participates in chunk replication
- Repairs corrupted chunks from replicas
- Reports available disk space and request statistics

### Client

The client provides a user interface to interact with the DFS, allowing users to:
- Store files by breaking them into chunks
- Retrieve files in parallel from storage nodes
- List files stored in the system
- Delete files
- View node status information

## Protocol Buffer Implementation

The system uses Protocol Buffers for all communication between components:

1. **Message Definitions**: Created in `dfs/proto/dfs.proto` with message types for:
   - Heartbeats
   - Storage requests/responses
   - Retrieval requests/responses
   - Chunk storage/retrieval
   - File listing and deletion
   - Node status information

2. **Serialization/Deserialization**: Using Protocol Buffers for efficient binary encoding:
   - Using `proto.Marshal()` for serialization
   - Using `proto.Unmarshal()` for deserialization
   - Maintaining the same message structure but with more efficient binary encoding

3. **Type Safety**: Protocol Buffers provide stronger type checking at compile time

## Key Features

### Chunking
Files are split into configurable-sized chunks (default 64MB), enabling:
- Parallel operations
- Better distribution across storage nodes
- Efficient storage management

### Replication
Each chunk is replicated 3 times across different storage nodes for:
- Fault tolerance
- Read parallelism
- Data durability

### Fault Detection & Recovery
- Heartbeat mechanism detects node failures
- Automatic re-replication maintains the replication factor
- Checksum verification detects corrupted chunks
- Corrupted chunks are repaired from replicas

### Parallel Operations
- Multi-threaded chunk transfers
- Pipeline replication for efficiency
- Parallel chunk retrieval and reconstruction

## Benefits of Protocol Buffers

1. **Performance**: More efficient serialization with smaller message sizes
2. **Type Safety**: Strong typing helps catch errors at compile time
3. **Versioning**: Better support for evolving message formats
4. **Cross-Language Support**: Can be used with multiple programming languages
5. **Schema Definition**: Clear definition of message structures

## How to Use

### Building the System

```bash
cd dfs
go build -o controller ./controller
go build -o storage ./storage
go build -o client ./client
```

### Starting the Controller

```bash
./controller -port 8000
```

### Starting Storage Nodes

Start multiple storage nodes, each with a unique ID (port) and data directory:

```bash
./storage -id 8001 -controller localhost:8000 -data ./data1
./storage -id 8002 -controller localhost:8000 -data ./data2
./storage -id 8003 -controller localhost:8000 -data ./data3
```

### Using the Client

```bash
./client -controller localhost:8000
```

The client provides an interactive interface with the following commands:

- `store <filepath> [chunk_size]` - Store a file in the DFS
- `retrieve <filename> <output_path>` - Retrieve a file from the DFS
- `list` - List all files in the DFS
- `delete <filename>` - Delete a file from the DFS
- `status` - Show status of all storage nodes
- `exit` - Exit the client

## Implementation Details

### Chunk Size

The default chunk size is 64MB, which provides a good balance between:
- Minimizing metadata overhead
- Enabling efficient parallel transfers
- Providing flexibility for different file sizes

### Replication Strategy

Each chunk is replicated 3 times (by default) for fault tolerance:
- Primary copy on the node with the most available space
- Secondary copies on different nodes
- Pipeline replication: client → node1 → node2 → node3

### Failure Detection

- Heartbeats sent every 5 seconds
- Node considered failed after 15 seconds without heartbeat
- Automatic re-replication of chunks from failed nodes

### Corruption Handling

- SHA-256 checksums for each chunk
- Verification on every read
- Automatic repair using replicas