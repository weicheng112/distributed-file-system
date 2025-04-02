package main

import (
	"fmt"
	"log"
	"sort"
	"time"

	"dfs/common"
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
			Address:          heartbeat.NodeHostname + ":" + heartbeat.NodeId,
			ReplicatedChunks: make(map[string][]int),
		}
		c.nodes[heartbeat.NodeId] = node
		log.Printf("New node joined: %s (Address: %s)", heartbeat.NodeId, node.Address)
	}

	// Update node status
	node.FreeSpace = heartbeat.FreeSpace
	node.RequestsHandled = heartbeat.RequestsProcessed
	node.LastHeartbeat = time.Now()
// Process any new files reported
if len(heartbeat.NewFiles) > 0 {
	log.Printf("Node %s reported %d new files: %v", heartbeat.NodeId, len(heartbeat.NewFiles), heartbeat.NewFiles)
}

	
	for _, filename := range heartbeat.NewFiles {
		log.Printf("Processing reported file: %s from node %s (Address: %s)", filename, heartbeat.NodeId, node.Address)
		
		// Update the node's ReplicatedChunks map
		fileMetadata, exists := c.files[filename]
		if exists {
			log.Printf("File %s exists in controller's files map with %d chunks", filename, len(fileMetadata.Chunks))
			// Find chunks of this file stored on this node
			for chunkNum, nodes := range fileMetadata.Chunks {
				// log.Printf("  Checking chunk %d which is stored on nodes: %v", chunkNum, nodes)
				for _, storedNode := range nodes {
					// log.Printf("  Comparing stored node %s with reporting node %s (Address: %s)", storedNode, heartbeat.NodeId, node.Address)
					// Match against both the node ID and the node address
					if storedNode == heartbeat.NodeId || storedNode == node.Address {
						// log.Printf("  Match found! Node %s has chunk %d of file %s", heartbeat.NodeId, chunkNum, filename)
						// This chunk is stored on this node
						if _, exists := node.ReplicatedChunks[filename]; !exists {
							// log.Printf("  Creating new entry for file %s in node's ReplicatedChunks map", filename)
							node.ReplicatedChunks[filename] = []int{}
						}
						
						// Check if this chunk is already in the list
						chunkExists := false
						for _, existingChunk := range node.ReplicatedChunks[filename] {
							if existingChunk == chunkNum {
								chunkExists = true
								break
							}
						}
						
						// Add the chunk if it doesn't exist
						if !chunkExists {
							node.ReplicatedChunks[filename] = append(node.ReplicatedChunks[filename], chunkNum)
							// log.Printf("  Updated node %s ReplicatedChunks: added chunk %d of file %s",
							// 	heartbeat.NodeId, chunkNum, filename)
						} else {
							// log.Printf("  Chunk %d of file %s already exists in node's ReplicatedChunks map", chunkNum, filename)
						}
					}
				}
			}
		} else {
			// File doesn't exist in the controller's files map
			// Create a new entry for this file
			log.Printf("File %s does not exist in controller's files map, creating new entry", filename)
			log.Printf("Creating new file entry for %s reported by node %s", filename, heartbeat.NodeId)
			
			// Create a new file metadata entry
			c.files[filename] = &FileMetadata{
				Size:      0, // We don't know the size yet
				ChunkSize: int(common.DefaultChunkSize),
				Chunks:    make(map[int][]string),
			}
			// Add the node to the list of nodes that have chunk 0 of this file
			c.files[filename].Chunks[0] = []string{node.Address}
			log.Printf("Added node %s (Address: %s) to list of nodes for chunk 0 of file %s", heartbeat.NodeId, node.Address, filename)
			
			
			// Update the node's ReplicatedChunks map
			if _, exists := node.ReplicatedChunks[filename]; !exists {
				log.Printf("Creating new entry for file %s in node's ReplicatedChunks map", filename)
				node.ReplicatedChunks[filename] = []int{}
			}
			
			// Add chunk 0 to the node's ReplicatedChunks map
			node.ReplicatedChunks[filename] = append(node.ReplicatedChunks[filename], 0)
			log.Printf("Updated node %s ReplicatedChunks: added chunk 0 of file %s", heartbeat.NodeId, filename)
			
			// Log the current state of the controller's files map
			log.Printf("Controller's files map now contains %d files", len(c.files))
			for fname, meta := range c.files {
				log.Printf("  File: %s, Chunks: %d", fname, len(meta.Chunks))
				for chunkNum, nodes := range meta.Chunks {
					log.Printf("    Chunk %d: %v", chunkNum, nodes)
				}
			}
		}
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
	log.Printf("File %s metadata:", request.Filename)
	log.Printf("  Size: %d bytes", request.FileSize)
	log.Printf("  Chunk Size: %d bytes", request.ChunkSize)
	log.Printf("  Number of Chunks: %d", numChunks)
	log.Printf("  Chunk Placements:")
	for chunkNum, nodes := range c.files[request.Filename].Chunks {
		log.Printf("    Chunk %d: %v", chunkNum, nodes)
	}
	
	// Print node information
	log.Println("Storage Node Information:")
	for nodeID, info := range c.nodes {
		log.Printf("  Node: %s", nodeID)
		log.Printf("    Address: %s", info.Address)
		log.Printf("    Free Space: %d bytes (%.2f GB)", info.FreeSpace, float64(info.FreeSpace)/(1024*1024*1024))
		log.Printf("    Requests Handled: %d", info.RequestsHandled)
		log.Printf("    Last Heartbeat: %s", info.LastHeartbeat.Format(time.RFC3339))
		
		// Print chunks stored on this node
		log.Printf("    Chunks Stored:")
		totalChunks := 0
		for filename, chunks := range info.ReplicatedChunks {
			log.Printf("      File: %s, Chunks: %v", filename, chunks)
			totalChunks += len(chunks)
		}
		log.Printf("    Total Chunks: %d", totalChunks)
	}
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
	for chunkNum, nodeIDs := range metadata.Chunks {
		// Convert node IDs to addresses
		nodeAddresses := make([]string, len(nodeIDs))
		for i, nodeID := range nodeIDs {
			if node, exists := c.nodes[nodeID]; exists {
				nodeAddresses[i] = node.Address
			} else {
				// If node doesn't exist anymore, use the nodeID as fallback
				nodeAddresses[i] = nodeID
			}
		}
		
		chunk := &pb.ChunkLocation{
			ChunkNumber:   uint32(chunkNum),
			StorageNodes: nodeAddresses,
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
	var availableNodeIDs []string
	for nodeID, info := range c.nodes {
		if info.FreeSpace >= uint64(chunkSize) {
			availableNodeIDs = append(availableNodeIDs, nodeID)
		}
	}

	// Sort nodes by available space (descending)
	sort.Slice(availableNodeIDs, func(i, j int) bool {
		return c.nodes[availableNodeIDs[i]].FreeSpace > c.nodes[availableNodeIDs[j]].FreeSpace
	})

	// Select top N nodes where N is replication factor
	if len(availableNodeIDs) > c.replicationFactor {
		availableNodeIDs = availableNodeIDs[:c.replicationFactor]
	}

	// Convert node IDs to addresses
	nodeAddresses := make([]string, len(availableNodeIDs))
	for i, nodeID := range availableNodeIDs {
		nodeAddresses[i] = c.nodes[nodeID].Address
	}

	return nodeAddresses
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


	log.Println("===== STORAGE NODE STATUS =====")
	log.Printf("Controller has %d active nodes registered", len(c.nodes))
	
	// Log the current state of the controller's files map with more detail
	log.Printf("\n===== FILE REPLICATION STATUS =====")
	log.Printf("Controller's files map contains %d files", len(c.files))
	for filename, meta := range c.files {
		totalChunks := len(meta.Chunks)
		log.Printf("\n  File: %s", filename)
		log.Printf("    Size: %d bytes (%.2f MB)", meta.Size, float64(meta.Size)/(1024*1024))
		log.Printf("    ChunkSize: %d bytes (%.2f MB)", meta.ChunkSize, float64(meta.ChunkSize)/(1024*1024))
		log.Printf("    Total Chunks: %d", totalChunks)
		log.Printf("    Replication Status:")
		
		// Count chunks with different replication levels
		replicationCounts := make(map[int]int)
		for chunkNum, nodes := range meta.Chunks {
			replicationCounts[len(nodes)]++
			log.Printf("      Chunk %d: %d replicas on nodes: %v", chunkNum, len(nodes), nodes)
		}
		
		// Summary of replication levels
		log.Printf("    Replication Summary:")
		for repLevel, count := range replicationCounts {
			percentage := float64(count) / float64(totalChunks) * 100
			log.Printf("      %d chunks (%.1f%%) have %d replicas", count, percentage, repLevel)
		}
	}
	
	for nodeID, info := range c.nodes {
		log.Printf("  Node: %s", nodeID)
		log.Printf("    Address: %s", info.Address)
		log.Printf("    Free Space: %d bytes (%.2f GB)", info.FreeSpace, float64(info.FreeSpace)/(1024*1024*1024))
		log.Printf("    Requests Handled: %d", info.RequestsHandled)
		log.Printf("    Last Heartbeat: %s", info.LastHeartbeat.Format(time.RFC3339))
		
		// Print chunks stored on this node
		log.Printf("    Chunks Stored (from ReplicatedChunks map):")
		totalChunks := 0
		if info.ReplicatedChunks == nil {
			log.Printf("      ReplicatedChunks map is nil!")
		} else if len(info.ReplicatedChunks) == 0 {
			log.Printf("      ReplicatedChunks map is empty")
		} else {
			for filename, chunks := range info.ReplicatedChunks {
				log.Printf("      File: %s, Chunks: %v", filename, chunks)
				totalChunks += len(chunks)
			}
		}
		log.Printf("    Total Chunks: %d", totalChunks)
		
		// Cross-check with the controller's files map with more detailed information
		log.Printf("    Cross-checking with controller's files map:")
		crossCheckTotal := 0
		fileChunks := make(map[string][]int)
		
		for filename, meta := range c.files {
			nodeChunks := []int{}
			for chunkNum, nodes := range meta.Chunks {
				for _, node := range nodes {
					if node == nodeID || node == info.Address {
						nodeChunks = append(nodeChunks, chunkNum)
						crossCheckTotal++
					}
				}
			}
			if len(nodeChunks) > 0 {
				fileChunks[filename] = nodeChunks
			}
		}
		
		// Print detailed information about which files and chunks this node is responsible for
		if len(fileChunks) > 0 {
			log.Printf("    This node is responsible for chunks in %d files:", len(fileChunks))
			for filename, chunks := range fileChunks {
				percentage := float64(len(chunks)) / float64(len(c.files[filename].Chunks)) * 100
				log.Printf("      File: %s - %d/%d chunks (%.1f%%): %v",
					filename, len(chunks), len(c.files[filename].Chunks), percentage, chunks)
			}
		} else {
			log.Printf("    This node is not responsible for any chunks")
		}
		
		log.Printf("    Total Chunks (cross-check): %d", crossCheckTotal)
	}

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	return responseData, nil
}