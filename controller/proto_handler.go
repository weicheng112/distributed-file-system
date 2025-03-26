package main

import (
	"fmt"
	"log"
	"sort"
	"time"

	pb "dfs/proto"

	"google.golang.org/protobuf/proto"
)

// handleHeartbeat processes a heartbeat message from a storage node
// This updates the node's status and handles any new files reported
func (c *Controller) handleHeartbeat(data []byte) error {
	heartbeat := &pb.Heartbeat{}
	if err := proto.Unmarshal(data, heartbeat); err != nil {
		return fmt.Errorf("failed to unmarshal heartbeat: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Update node information
	node, exists := c.nodes[heartbeat.NodeId]
	if !exists {
		// This is a new node
		node = &NodeInfo{
			ID:               heartbeat.NodeId,
			Address:          "localhost:" + heartbeat.NodeId, // Prepend localhost to make a valid address
			ReplicatedChunks: make(map[string][]int),
		}
		c.nodes[heartbeat.NodeId] = node
		log.Printf("New node joined: %s", heartbeat.NodeId)
	}

	// Update node status
	node.FreeSpace = heartbeat.FreeSpace
	node.RequestsHandled = heartbeat.RequestsProcessed
	node.LastHeartbeat = time.Now()

	// Process any new files reported
	for _, filename := range heartbeat.NewFiles {
		log.Printf("Node %s reported new file: %s", heartbeat.NodeId, filename)
	}

	return nil
}

// handleStorageRequest processes a storage request from a client
// It determines where to store each chunk of the file
func (c *Controller) handleStorageRequest(data []byte) ([]byte, error) {
	request := &pb.StorageRequest{}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal storage request: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if file already exists
	if _, exists := c.files[request.Filename]; exists {
		return nil, fmt.Errorf("file already exists")
	}

	// Calculate number of chunks needed
	numChunks := (request.FileSize + uint64(request.ChunkSize) - 1) / uint64(request.ChunkSize)

	// Create chunk placements
	response := &pb.StorageResponse{
		ChunkPlacements: make([]*pb.ChunkPlacement, 0, int(numChunks)),
	}

	// For each chunk, select storage nodes
	for chunkNum := uint64(0); chunkNum < numChunks; chunkNum++ {
		nodes := c.selectStorageNodes(int(request.ChunkSize))
		if len(nodes) < c.replicationFactor {
			return nil, fmt.Errorf("not enough storage nodes available")
		}

		placement := &pb.ChunkPlacement{
			ChunkNumber:   uint32(chunkNum),
			StorageNodes: nodes,
		}
		response.ChunkPlacements = append(response.ChunkPlacements, placement)

		// Store chunk placements in metadata
		if _, exists := c.files[request.Filename]; !exists {
			c.files[request.Filename] = &FileMetadata{
				Size:      int64(request.FileSize),
				ChunkSize: int(request.ChunkSize),
				Chunks:    make(map[int][]string),
			}
		}
		c.files[request.Filename].Chunks[int(chunkNum)] = nodes
	}

	// Print the metadata map for the file
	// log.Printf("File %s metadata:", request.Filename)
	// log.Printf("  Size: %d bytes", request.FileSize)
	// log.Printf("  Chunk Size: %d bytes", request.ChunkSize)
	// log.Printf("  Number of Chunks: %d", numChunks)
	// log.Printf("  Chunk Placements:")
	for chunkNum, nodes := range c.files[request.Filename].Chunks {
		log.Printf("    Chunk %d: %v", chunkNum, nodes)
	}
	
	// Print the entire metadata map
	c.printMetadataMap()

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	return responseData, nil
}

// handleRetrievalRequest processes a file retrieval request from a client
// It returns the locations of all chunks for the requested file
func (c *Controller) handleRetrievalRequest(data []byte) ([]byte, error) {
	request := &pb.RetrievalRequest{}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal retrieval request: %v", err)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check if file exists
	metadata, exists := c.files[request.Filename]
	if !exists {
		return nil, fmt.Errorf("file not found")
	}

	// Create chunk locations response
	response := &pb.RetrievalResponse{
		Chunks: make([]*pb.ChunkLocation, 0, len(metadata.Chunks)),
	}

	// Add locations for each chunk
	for chunkNum, nodes := range metadata.Chunks {
		chunk := &pb.ChunkLocation{
			ChunkNumber:   uint32(chunkNum),
			StorageNodes: nodes,
		}
		response.Chunks = append(response.Chunks, chunk)
	}

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	return responseData, nil
}

// selectStorageNodes selects nodes for storing a new chunk
// It prioritizes nodes with more available space
func (c *Controller) selectStorageNodes(chunkSize int) []string {
	var availableNodes []string
	for nodeID, info := range c.nodes {
		if info.FreeSpace >= uint64(chunkSize) {
			availableNodes = append(availableNodes, nodeID)
		}
	}

	// Sort nodes by available space (descending)
	sort.Slice(availableNodes, func(i, j int) bool {
		return c.nodes[availableNodes[i]].FreeSpace > c.nodes[availableNodes[j]].FreeSpace
	})

	// Select top N nodes where N is replication factor
	if len(availableNodes) > c.replicationFactor {
		availableNodes = availableNodes[:c.replicationFactor]
	}

	return availableNodes
}

// handleDeleteRequest processes a file deletion request
func (c *Controller) handleDeleteRequest(data []byte) ([]byte, error) {
	request := &pb.DeleteRequest{}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal delete request: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if file exists
	metadata, exists := c.files[request.Filename]
	if !exists {
		return nil, fmt.Errorf("file not found")
	}

	// Create response
	response := &pb.DeleteResponse{
		Success: true,
	}

	// Remove file metadata
	delete(c.files, request.Filename)

	// Update node chunk information
	for _, nodes := range metadata.Chunks {
		for _, nodeID := range nodes {
			if node, exists := c.nodes[nodeID]; exists {
				delete(node.ReplicatedChunks, request.Filename)
			}
		}
	}

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	return responseData, nil
}

// handleListRequest processes a file listing request
func (c *Controller) handleListRequest(data []byte) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	response := &pb.ListFilesResponse{
		Files: make([]*pb.FileInfo, 0, len(c.files)),
	}

	for filename, metadata := range c.files {
		fileInfo := &pb.FileInfo{
			Filename:  filename,
			Size:      uint64(metadata.Size),
			NumChunks: uint32(len(metadata.Chunks)),
		}
		response.Files = append(response.Files, fileInfo)
	}

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	return responseData, nil
}

// handleNodeStatusRequest processes a node status request
func (c *Controller) handleNodeStatusRequest(data []byte) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	response := &pb.NodeStatusResponse{
		Nodes: make([]*pb.NodeInfo, 0, len(c.nodes)),
	}

	var totalSpace uint64
	for _, node := range c.nodes {
		nodeInfo := &pb.NodeInfo{
			NodeId:           node.ID,
			FreeSpace:       node.FreeSpace,
			RequestsProcessed: node.RequestsHandled,
		}
		response.Nodes = append(response.Nodes, nodeInfo)
		totalSpace += node.FreeSpace
	}

	response.TotalSpace = totalSpace

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	return responseData, nil
}