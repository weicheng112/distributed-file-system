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

// getOutboundIP gets the preferred outbound IP of this machine
func getOutboundIP() string {
	// Method 1: Try to get the IP by creating a UDP connection (doesn't actually connect)
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err == nil {
		defer conn.Close()
		localAddr := conn.LocalAddr().(*net.UDPAddr)
		return localAddr.IP.String()
	}
	log.Printf("Failed to determine outbound IP via UDP: %v", err)
	
	// Method 2: Try to get the IP by listing network interfaces
	ifaces, err := net.Interfaces()
	if err == nil {
		for _, iface := range ifaces {
			// Skip loopback and down interfaces
			if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
				continue
			}
			
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			
			for _, addr := range addrs {
				switch v := addr.(type) {
				case *net.IPNet:
					if !v.IP.IsLoopback() && v.IP.To4() != nil {
						return v.IP.String()
					}
				}
			}
		}
	}
	log.Printf("Failed to determine outbound IP via network interfaces: %v", err)
	
	// Method 3: Fallback to hostname
	hostname, err := os.Hostname()
	if err == nil {
		log.Printf("Current IP Addr %s", hostname)
		return hostname
	}
	
	// Last resort
	return "localhost"
}

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
	// Get the IP address instead of hostname for better network addressability
	ipAddress := getOutboundIP()
	// log.Printf("Current IP Addr %s", ipAddress)
	heartbeat := &pb.Heartbeat{
		NodeId:           n.nodeID,
		NodeHostname:     ipAddress,
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
	// Ensure the node address includes the hostname
	nodeAddr := nodeID
	// The controller should now provide the full address (hostname:port)
	// If it doesn't contain a colon, fall back to the old behavior
	if !strings.Contains(nodeAddr, ":") {
		log.Printf("Warning: Node address %s does not contain a hostname, falling back to localhost", nodeAddr)
		nodeAddr = "localhost:" + nodeAddr
	}
	
	conn, err := net.Dial("tcp", nodeAddr)
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
		// Ensure the node address includes the hostname
		nodeAddr := replicaNode
		// The controller should now provide the full address (hostname:port)
		// If it doesn't contain a colon, fall back to the old behavior
		if !strings.Contains(nodeAddr, ":") {
			log.Printf("Warning: Node address %s does not contain a hostname, falling back to localhost", nodeAddr)
			nodeAddr = "localhost:" + nodeAddr
		}
		
		conn, err := net.Dial("tcp", nodeAddr)
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
	
	// Ensure reportedFiles map is initialized
	if n.reportedFiles == nil {
		n.reportedFiles = make(map[string]bool)
		log.Printf("Initialized reportedFiles map in getNewFiles")
	}
	
	// Log the current state of the reportedFiles map
	log.Printf("Current reportedFiles map contains %d entries", len(n.reportedFiles))
	
	// Check for new files
	for chunkKey := range n.chunks {
		// Extract the filename correctly, preserving the full filename
		// The chunk key format is "filename_chunknumber"
		lastUnderscore := strings.LastIndex(chunkKey, "_")
		if lastUnderscore == -1 {
			log.Printf("Warning: Invalid chunk key format: %s", chunkKey)
			continue
		}
		
		filename := chunkKey[:lastUnderscore]
		if !n.reportedFiles[filename] {
			log.Printf("Found new unreported file: %s", filename)
			newFiles = append(newFiles, filename)
			n.reportedFiles[filename] = true
			log.Printf("Marked file %s as reported", filename)
			
			// Save the updated reportedFiles map immediately
			go func() {
				if err := n.saveReportedFiles(); err != nil {
					log.Printf("Warning: failed to save reported files: %v", err)
				}
			}()
		}
	}
	
	if len(newFiles) > 0 {
		log.Printf("Reporting %d new files: %v", len(newFiles), newFiles)
	} else {
		log.Printf("No new files to report")
	}
	
	return newFiles
}

// saveReportedFiles saves just the reported files map to disk
func (n *StorageNode) saveReportedFiles() error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	// Ensure reportedFiles map is initialized
	if n.reportedFiles == nil {
		n.reportedFiles = make(map[string]bool)
		log.Printf("Initialized reportedFiles map in saveReportedFiles")
	}
	
	// Save reported files
	reportedPath := filepath.Join(n.dataDir, "reported_files.json")
	reportedFile, err := os.Create(reportedPath)
	if err != nil {
		return fmt.Errorf("failed to create reported files file: %v", err)
	}
	defer reportedFile.Close()
	
	// Create a more informative structure to save
	type ReportedFilesInfo struct {
		Files     map[string]bool `json:"files"`
		Timestamp string          `json:"last_updated"`
		NodeID    string          `json:"node_id"`
	}
	
	info := ReportedFilesInfo{
		Files:     n.reportedFiles,
		Timestamp: time.Now().Format(time.RFC3339),
		NodeID:    n.nodeID,
	}
	
	reportedEncoder := json.NewEncoder(reportedFile)
	if err := reportedEncoder.Encode(info); err != nil {
		return fmt.Errorf("failed to encode reported files: %v", err)
	}
	
	log.Printf("Saved reported files info to %s", reportedPath)
	return nil
}

// saveMetadata saves the current chunk metadata to disk
func (n *StorageNode) saveMetadata() error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Save chunks metadata
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
	
	log.Printf("Saved metadata for %d chunks to %s", len(n.chunks), metadataPath)

	// Also save reported files
	return n.saveReportedFiles()
}

// loadMetadata loads chunk metadata and reported files from disk
func (n *StorageNode) loadMetadata() error {
	// Load chunks metadata
	metadataPath := filepath.Join(n.dataDir, "metadata.json")
	
	// Check if metadata file exists
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		// No metadata file, start with empty metadata
		log.Printf("No metadata file found at %s, starting with empty metadata", metadataPath)
	} else {
		file, err := os.Open(metadataPath)
		if err != nil {
			return fmt.Errorf("failed to open metadata file: %v", err)
		}
		defer file.Close()
		
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&n.chunks); err != nil {
			return fmt.Errorf("failed to decode metadata: %v", err)
		}
		
		log.Printf("Loaded metadata for %d chunks", len(n.chunks))
	}
	
	// Load reported files
	reportedPath := filepath.Join(n.dataDir, "reported_files.json")
	
	// Check if reported files file exists
	if _, err := os.Stat(reportedPath); os.IsNotExist(err) {
		// No reported files file, start with empty map
		log.Printf("No reported files file found at %s, starting with empty map", reportedPath)
		
		// Ensure reportedFiles map is initialized
		if n.reportedFiles == nil {
			n.reportedFiles = make(map[string]bool)
			log.Printf("Initialized reportedFiles map")
		}
		
		// If reportExisting is false, mark all existing files as reported to prevent re-reporting them
		// If reportExisting is true, leave them unmarked so they will be reported
		if !n.reportExisting {
			log.Printf("Not reporting existing files on startup (report-existing=false)")
			for chunkKey := range n.chunks {
				// Extract the filename correctly, preserving the full filename
				lastUnderscore := strings.LastIndex(chunkKey, "_")
				if lastUnderscore == -1 {
					log.Printf("Warning: Invalid chunk key format: %s", chunkKey)
					continue
				}
				
				filename := chunkKey[:lastUnderscore]
				n.reportedFiles[filename] = true
				log.Printf("Marked existing file %s as reported (will not be reported to controller)", filename)
			}
		} else {
			log.Printf("Will report existing files on startup (report-existing=true)")
		}
		
		// Save the initial reported files map
		if err := n.saveReportedFiles(); err != nil {
			log.Printf("Warning: failed to save initial reported files: %v", err)
		}
	} else {
		reportedFile, err := os.Open(reportedPath)
		if err != nil {
			return fmt.Errorf("failed to open reported files file: %v", err)
		}
		defer reportedFile.Close()
		
		// Try to decode as the new format first
		type ReportedFilesInfo struct {
			Files     map[string]bool `json:"files"`
			Timestamp string          `json:"last_updated"`
			NodeID    string          `json:"node_id"`
		}
		
		var info ReportedFilesInfo
		reportedDecoder := json.NewDecoder(reportedFile)
		if err := reportedDecoder.Decode(&info); err == nil {
			// Successfully decoded new format
			if n.reportExisting {
				// If we're reporting existing files, clear the reported files map
				n.reportedFiles = make(map[string]bool)
				log.Printf("Cleared reported files map to report existing files (report-existing=true)")
			} else {
				// Otherwise, use the loaded map
				n.reportedFiles = info.Files
				log.Printf("Loaded %d reported files from new format (last updated: %s)",
					len(n.reportedFiles), info.Timestamp)
			}
		} else {
			// Try old format
			reportedFile.Seek(0, 0) // Reset file position
			var oldFormat map[string]bool
			reportedDecoder = json.NewDecoder(reportedFile)
			if err := reportedDecoder.Decode(&oldFormat); err != nil {
				return fmt.Errorf("failed to decode reported files: %v", err)
			}
			
			if n.reportExisting {
				// If we're reporting existing files, clear the reported files map
				n.reportedFiles = make(map[string]bool)
				log.Printf("Cleared reported files map to report existing files (report-existing=true)")
			} else {
				// Otherwise, use the loaded map
				n.reportedFiles = oldFormat
				log.Printf("Loaded %d reported files from old format", len(n.reportedFiles))
			}
			
			// Save in new format
			if err := n.saveReportedFiles(); err != nil {
				log.Printf("Warning: failed to save reported files in new format: %v", err)
			}
		}
	}
	
	return nil
}