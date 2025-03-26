# How to Store Files in the Distributed File System

This guide demonstrates how to store files in our distributed file system using the client application.

## Prerequisites

Before storing files, make sure you have:

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

3. Started at least one storage node (preferably three for replication):
   ```bash
   ./storage -id 8001 -controller localhost:8000 -data ./data1
   ./storage -id 8002 -controller localhost:8000 -data ./data2
   ./storage -id 8003 -controller localhost:8000 -data ./data3
   ```

## Storing a File

### Method 1: Using the Interactive Client

1. Start the client:
   ```bash
   ./client -controller localhost:8000
   ```

2. At the client prompt, use the `store` command:
   ```
   Enter command: store /path/to/your/file.txt
   ```

   This will store the file using the default chunk size (64MB).

3. To specify a custom chunk size (in bytes), add it as a second parameter:
   ```
   Enter command: store /path/to/your/file.txt 1048576
   ```
   This example uses a 1MB chunk size (1048576 bytes).

### Method 2: Programmatic Usage

If you want to store files programmatically from your own Go code, you can use the client library:

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
    
    // Store a file with default chunk size
    err := dfsClient.StoreFile("/path/to/your/file.txt", "file.txt", 0)
    if err != nil {
        log.Fatalf("Failed to store file: %v", err)
    }
    
    fmt.Println("File stored successfully")
}
```

## What Happens During File Storage

When you store a file, the following steps occur:

1. **File Chunking**: The client breaks the file into chunks of the specified size.

2. **Metadata Request**: The client sends a storage request to the controller with the filename, file size, and chunk size.

3. **Chunk Placement**: The controller determines where to store each chunk based on available space and returns a mapping of chunk numbers to storage node IDs.

4. **Chunk Transfer**: For each chunk:
   - The client sends the chunk to the primary storage node
   - The primary node stores the chunk and calculates its checksum
   - The primary node forwards the chunk to secondary nodes for replication
   - Each node confirms successful storage

5. **Completion**: Once all chunks are stored and replicated, the file is available for retrieval.

## Verifying Storage

To verify that your file was stored successfully:

1. Use the `list` command in the client to see all stored files:
   ```
   Enter command: list
   ```

2. You should see your file listed with its size and number of chunks.

3. You can also check the storage nodes' data directories to see the stored chunks:
   ```bash
   ls -la data1/
   ```

## Troubleshooting

If you encounter issues storing files:

1. **Not enough storage nodes**: Ensure you have at least as many storage nodes as your replication factor (default is 3).

2. **Connection issues**: Verify that the controller and storage nodes are running and accessible.

3. **Permission issues**: Make sure the client has read access to the source file and the storage nodes have write access to their data directories.

4. **Large files**: For very large files, you might need to increase the chunk size to reduce the number of chunks.