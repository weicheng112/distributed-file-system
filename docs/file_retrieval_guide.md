# How to Retrieve Files from the Distributed File System

This guide demonstrates how to retrieve files from our distributed file system using the client application.

## Prerequisites

Before retrieving files, make sure you have:

1. Built all components:
   ```bash
   cd dfs
   go build -o controller ./controller
   go build -o storage ./storage
   go build -o client ./client
   ```

2. Started the controller:
   ```bash
   ./controller -port 8000
   ```

3. Started the storage nodes where your file chunks are stored:
   ```bash
   ./storage -id 8001 -controller localhost:8000 -data ./data1
   ./storage -id 8002 -controller localhost:8000 -data ./data2
   ./storage -id 8003 -controller localhost:8000 -data ./data3
   ```

## Retrieving a File

### Method 1: Using the Interactive Client

1. Start the client:
   ```bash
   ./client -controller localhost:8000
   ```

2. First, you can list available files to see what's stored in the system:
   ```
   Enter command: list
   ```

3. At the client prompt, use the `retrieve` command with the filename and destination path:
   ```
   Enter command: retrieve file.txt /path/to/save/retrieved_file.txt
   ```

   This will retrieve the file and save it to the specified location.

### Method 2: Programmatic Usage

If you want to retrieve files programmatically from your own Go code, you can use the client library:

```go
package main

import (
    "fmt"
    "log"
    
    "dfs/client"
)

func main() {
    // Create a new client
    dfsClient := client.NewClient("localhost:8000")
    
    // Retrieve a file
    err := dfsClient.RetrieveFile("file.txt", "/path/to/save/retrieved_file.txt")
    if err != nil {
        log.Fatalf("Failed to retrieve file: %v", err)
    }
    
    fmt.Println("File retrieved successfully")
}
```

## What Happens During File Retrieval

When you retrieve a file, the following steps occur:

1. **Metadata Request**: The client sends a retrieval request to the controller with the filename.

2. **Chunk Location Response**: The controller returns the locations of all chunks for the requested file, including all replicas.

3. **Parallel Chunk Retrieval**: For each chunk:
   - The client creates a separate thread to retrieve the chunk
   - The client tries to retrieve the chunk from one of the available storage nodes
   - If a node fails or returns corrupted data, the client tries another replica
   - The storage node verifies the chunk's checksum before sending it

4. **File Reassembly**: Once all chunks are retrieved, the client reassembles them in the correct order to reconstruct the original file.

5. **Completion**: The reassembled file is saved to the specified output path.

## Fault Tolerance During Retrieval

The retrieval process is designed to be fault-tolerant:

1. **Node Failures**: If a storage node is down, the client will automatically try to retrieve the chunk from another node that has a replica.

2. **Corruption Detection**: If a chunk's checksum doesn't match (indicating corruption), the client will:
   - Reject the corrupted chunk
   - Try to retrieve it from another replica
   - The storage node may also attempt to repair its corrupted copy from a replica

3. **Parallel Retrieval**: By retrieving chunks in parallel, the system can handle slow or unresponsive nodes without significantly impacting overall performance.

## Verifying Retrieval

To verify that your file was retrieved correctly:

1. Check that the file exists at the specified output path:
   ```bash
   ls -la /path/to/save/retrieved_file.txt
   ```

2. Compare the retrieved file with the original (if available):
   ```bash
   diff /path/to/original/file.txt /path/to/save/retrieved_file.txt
   ```

## Troubleshooting

If you encounter issues retrieving files:

1. **File not found**: Verify that the file exists in the system using the `list` command.

2. **Missing chunks**: If some storage nodes are down and they were the only ones with certain chunks, retrieval may fail. Ensure enough nodes are running.

3. **Permission issues**: Make sure the client has write access to the destination directory.

4. **Corrupted chunks**: If all replicas of a chunk are corrupted, retrieval will fail. This indicates a serious system issue that may require manual intervention.