package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dfs/common"
	pb "dfs/proto"

	"google.golang.org/protobuf/proto"
)

// We're now using the Protocol Buffer generated types from dfs/proto

// connectToController establishes a connection to the controller
func (n *StorageNode) connectToController() error {
	// Connect to the controller
	conn, err := net.Dial("tcp", n.controllerAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %v", err)
	}
	n.controllerConn = conn
	
	log.Printf("Connected to controller at %s", n.controllerAddr)
	return nil
}

// sendHeartbeats periodically sends heartbeat messages to the controller
func (n *StorageNode) sendHeartbeats() {
	ticker := time.NewTicker(time.Duration(common.HeartbeatInterval) * time.Second)
	for range ticker.C {
		if err := n.sendHeartbeat(); err != nil {
			log.Printf("Failed to send heartbeat: %v", err)
			
			// Try to reconnect to controller
			if err := n.connectToController(); err != nil {
				log.Printf("Failed to reconnect to controller: %v", err)
				continue
			}
		}
	}
}

// sendHeartbeat sends a single heartbeat message to the controller
func (n *StorageNode) sendHeartbeat() error {
	// Get current disk space
	freeSpace, err := n.getFreeSpace()
	if err != nil {
		return fmt.Errorf("failed to get free space: %v", err)
	}

	// Create heartbeat message
	heartbeat := &pb.Heartbeat{
		NodeId:           n.nodeID,
		FreeSpace:        freeSpace,
		RequestsProcessed: n.requestsHandled,
		NewFiles:         n.getNewFiles(),
	}

	// Serialize message
	data, err := proto.Marshal(heartbeat)
	if err != nil {
		return fmt.Errorf("failed to marshal heartbeat: %v", err)
	}

	// Send to controller
	if err := common.WriteMessage(n.controllerConn, common.MsgTypeHeartbeat, data); err != nil {
		return fmt.Errorf("failed to send heartbeat: %v", err)
	}

	return nil
}

// handleChunkStore processes a chunk storage request
func (n *StorageNode) handleChunkStore(data []byte) ([]byte, error) {
	request := &pb.ChunkStoreRequest{}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal chunk store request: %v", err)
	}

	// Calculate checksum
	checksum := common.CalculateChecksum(request.Data)

	// Store chunk
	if err := n.storeChunk(request.Filename, int(request.ChunkNumber), request.Data, checksum); err != nil {
		return nil, fmt.Errorf("failed to store chunk: %v", err)
	}

	// Forward to replicas if needed
	for _, replicaNode := range request.ReplicaNodes {
		if replicaNode != n.nodeID {
			go n.forwardChunk(replicaNode, request.Filename, request.ChunkNumber, request.Data)
		}
	}

	// Create response
	response := &pb.ChunkStoreResponse{
		Success: true,
	}

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	return responseData, nil
}

// handleChunkRetrieve processes a chunk retrieval request
func (n *StorageNode) handleChunkRetrieve(data []byte) ([]byte, error) {
	request := &pb.ChunkRetrieveRequest{}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal chunk retrieve request: %v", err)
	}

	// Get chunk data
	chunkData, err := n.retrieveChunk(request.Filename, int(request.ChunkNumber))
	if err != nil {
		// Check if it's a corruption error
		if _, ok := err.(common.ChunkCorruptionError); ok {
			// Try to repair from replicas
			if err := n.repairChunk(request.Filename, int(request.ChunkNumber)); err != nil {
				return nil, fmt.Errorf("failed to repair corrupted chunk: %v", err)
			}
			// Try retrieval again
			chunkData, err = n.retrieveChunk(request.Filename, int(request.ChunkNumber))
			if err != nil {
				return nil, fmt.Errorf("failed to retrieve repaired chunk: %v", err)
			}
		} else {
			return nil, fmt.Errorf("failed to retrieve chunk: %v", err)
		}
	}

	// Create response
	response := &pb.ChunkRetrieveResponse{
		Data: chunkData,
	}

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	return responseData, nil
}

// forwardChunk forwards a chunk to another storage node
func (n *StorageNode) forwardChunk(nodeID string, filename string, chunkNum uint32, data []byte) error {
	// Connect to replica node
	conn, err := net.Dial("tcp", nodeID)
	if err != nil {
		return fmt.Errorf("failed to connect to replica node: %v", err)
	}
	defer conn.Close()

	// Create request
	request := &pb.ChunkStoreRequest{
		Filename:     filename,
		ChunkNumber:  chunkNum,
		Data:        data,
		// No further replicas to forward to
	}

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal forward request: %v", err)
	}

	// Send to replica
	if err := common.WriteMessage(conn, common.MsgTypeChunkStore, requestData); err != nil {
		return fmt.Errorf("failed to send chunk to replica: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("failed to read replica response: %v", err)
	}

	if msgType != common.MsgTypeChunkStore {
		return fmt.Errorf("unexpected response type from replica: %d", msgType)
	}

	response := &pb.ChunkStoreResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return fmt.Errorf("failed to unmarshal replica response: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("replica failed to store chunk: %s", response.Error)
	}

	return nil
}

// repairChunk attempts to repair a corrupted chunk from replicas
func (n *StorageNode) repairChunk(filename string, chunkNum int) error {
	// Get chunk metadata
	n.mu.RLock()
	metadata, exists := n.chunks[fmt.Sprintf("%s_%d", filename, chunkNum)]
	n.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no metadata for chunk")
	}

	// Try each replica
	for _, replicaNode := range metadata.Replicas {
		if replicaNode == n.nodeID {
			continue
		}

		// Connect to replica
		conn, err := net.Dial("tcp", replicaNode)
		if err != nil {
			continue
		}
		defer conn.Close()

		// Create request
		request := &pb.ChunkRetrieveRequest{
			Filename:    filename,
			ChunkNumber: uint32(chunkNum),
		}

		// Serialize request
		requestData, err := proto.Marshal(request)
		if err != nil {
			continue
		}

		// Send request
		if err := common.WriteMessage(conn, common.MsgTypeChunkRetrieve, requestData); err != nil {
			continue
		}

		// Read response
		msgType, responseData, err := common.ReadMessage(conn)
		if err != nil {
			continue
		}

		if msgType != common.MsgTypeChunkRetrieve {
			continue
		}

		response := &pb.ChunkRetrieveResponse{}
		if err := proto.Unmarshal(responseData, response); err != nil {
			continue
		}

		// Verify data
		checksum := common.CalculateChecksum(response.Data)
		if string(checksum) != string(metadata.Checksum) {
			continue
		}

		// Store repaired chunk
		if err := n.storeChunk(filename, chunkNum, response.Data, checksum); err != nil {
			continue
		}

		log.Printf("Successfully repaired chunk %s_%d from replica %s", filename, chunkNum, replicaNode)
		return nil
	}

	return fmt.Errorf("failed to repair chunk from any replica")
}

// getFreeSpace returns the available disk space
func (n *StorageNode) getFreeSpace() (uint64, error) {
	space, err := common.GetAvailableDiskSpace(n.dataDir)
	if err != nil {
		return 0, err
	}
	n.mu.Lock()
	n.freeSpace = space
	n.mu.Unlock()
	return space, nil
}

// getNewFiles returns a list of new files since last heartbeat
func (n *StorageNode) getNewFiles() []string {
	n.mu.Lock()
	defer n.mu.Unlock()

	newFiles := make([]string, 0)
	for chunkKey := range n.chunks {
		filename := strings.Split(chunkKey, "_")[0]
		if !n.reportedFiles[filename] {
			newFiles = append(newFiles, filename)
			n.reportedFiles[filename] = true
		}
	}
	return newFiles
}

// saveMetadata saves the current chunk metadata to disk
func (n *StorageNode) saveMetadata() error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	metadataPath := filepath.Join(n.dataDir, "metadata.json")
	file, err := os.Create(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to create metadata file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(n.chunks); err != nil {
		return fmt.Errorf("failed to encode metadata: %v", err)
	}

	return nil
}

// loadMetadata loads chunk metadata from disk
func (n *StorageNode) loadMetadata() error {
	metadataPath := filepath.Join(n.dataDir, "metadata.json")
	
	// Check if metadata file exists
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		// No metadata file, start with empty metadata
		return nil
	}
	
	file, err := os.Open(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to open metadata file: %v", err)
	}
	defer file.Close()
	
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&n.chunks); err != nil {
		return fmt.Errorf("failed to decode metadata: %v", err)
	}
	
	return nil
}