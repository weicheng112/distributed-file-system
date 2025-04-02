package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"

	"dfs/common"
)

// ChunkMetadata stores information about a stored chunk
type ChunkMetadata struct {
	Filename    string   // Name of the file this chunk belongs to
	ChunkNumber int      // Index of this chunk within the file
	Size        int64    // Size of the chunk in bytes
	Checksum    []byte   // SHA-256 checksum of the chunk data
	Replicas    []string // List of nodes that have replicas of this chunk
}

// StorageNode handles chunk storage and retrieval
type StorageNode struct {
	mu sync.RWMutex

	// Configuration
	nodeID         string // Unique identifier for this node (typically IP:port)
	controllerAddr string // Address of the controller
	dataDir        string // Directory where chunks are stored
	reportExisting bool   // Whether to report existing files on startup

	// Connection to controller
	controllerConn net.Conn

	// Chunk metadata
	chunks map[string]*ChunkMetadata // Key: filename_chunknumber

	// Statistics
	freeSpace       uint64 // Available disk space in bytes
	requestsHandled uint64 // Number of requests processed

	// Network
	listener net.Listener

	// Track reported files
	reportedFiles map[string]bool // Files that have been reported to the controller
}

// NewStorageNode creates a new storage node instance
func NewStorageNode(nodeID, controllerAddr, dataDir string, reportExisting bool) *StorageNode {
	return &StorageNode{
		nodeID:         nodeID,
		controllerAddr: controllerAddr,
		dataDir:        dataDir,
		reportExisting: reportExisting,
		chunks:         make(map[string]*ChunkMetadata),
		reportedFiles:  make(map[string]bool),
	}
}

// Start initializes the storage node and begins listening for connections
func (n *StorageNode) Start() error {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(n.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}

	// Load existing chunks metadata
	if err := n.loadMetadata(); err != nil {
		return fmt.Errorf("failed to load metadata: %v", err)
	}

	// Connect to controller
	if err := n.connectToController(); err != nil {
		return fmt.Errorf("failed to connect to controller: %v", err)
	}

	// Start heartbeat
	go n.sendHeartbeats()

	// Start listener for chunk operations
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", n.nodeID))
	if err != nil {
		return fmt.Errorf("failed to start listener: %v", err)
	}
	n.listener = listener

	log.Printf("Storage node started. ID: %s, Data dir: %s", n.nodeID, n.dataDir)

	// Accept and handle connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go n.handleConnection(conn)
	}
}

// handleConnection processes incoming connections from clients and other storage nodes
func (n *StorageNode) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Read message type and data
		msgType, data, err := common.ReadMessage(conn)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			return
		}

		var response []byte
		var respErr error

		// Handle different message types
		switch msgType {
		case common.MsgTypeChunkStore:
			response, respErr = n.handleChunkStore(data)
		case common.MsgTypeChunkRetrieve:
			response, respErr = n.handleChunkRetrieve(data)
		default:
			respErr = &common.ProtocolError{Message: fmt.Sprintf("unknown message type: %d", msgType)}
		}

		if respErr != nil {
			log.Printf("Error handling message type %d: %v", msgType, respErr)
			// Send error response if applicable
			if response != nil {
				if err := common.WriteMessage(conn, msgType, response); err != nil {
					log.Printf("Error sending error response: %v", err)
				}
			}
			return
		}

		// Send response if one was generated
		if response != nil {
			if err := common.WriteMessage(conn, msgType, response); err != nil {
				log.Printf("Error sending response: %v", err)
				return
			}
		}
	}
}

// storeChunk stores a chunk on disk with its checksum
func (n *StorageNode) storeChunk(filename string, chunkNum int, data []byte, checksum []byte) error {
	log.Printf("Storing chunk %d of file %s (size: %d bytes)", chunkNum, filename, len(data))
	
	// Create chunk file path
	chunkPath := filepath.Join(n.dataDir, fmt.Sprintf("%s_%d", filename, chunkNum))
	
	// Create chunk file
	file, err := os.Create(chunkPath)
	if err != nil {
		return fmt.Errorf("failed to create chunk file: %v", err)
	}
	defer file.Close()

	// Write checksum (32 bytes) followed by data
	if err := binary.Write(file, binary.LittleEndian, checksum); err != nil {
		return fmt.Errorf("failed to write checksum: %v", err)
	}
	
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write chunk data: %v", err)
	}

	// Update metadata
	n.mu.Lock()
	chunkKey := fmt.Sprintf("%s_%d", filename, chunkNum)
	isNewFile := false
	
	// Check if this is a new file that hasn't been reported yet
	if _, exists := n.reportedFiles[filename]; !exists {
		isNewFile = true
		log.Printf("New file detected: %s", filename)
	}
	
	n.chunks[chunkKey] = &ChunkMetadata{
		Filename:    filename,
		ChunkNumber: chunkNum,
		Size:        int64(len(data)),
		Checksum:    checksum,
	}
	n.requestsHandled++
	
	// Update free space
	freeSpace, err := common.GetAvailableDiskSpace(n.dataDir)
	if err != nil {
		log.Printf("Warning: failed to get free space: %v", err)
	} else {
		n.freeSpace = freeSpace
		log.Printf("Updated free space: %d bytes (%.2f GB)", n.freeSpace, float64(n.freeSpace)/(1024*1024*1024))
	}
	
	n.mu.Unlock()

	// Save metadata to disk
	if err := n.saveMetadata(); err != nil {
		log.Printf("Warning: failed to save metadata: %v", err)
	}
	
	log.Printf("Successfully stored chunk %d of file %s at %s", chunkNum, filename, chunkPath)
	
	if isNewFile {
		log.Printf("File %s will be reported in the next heartbeat", filename)
	}

	return nil
}

// retrieveChunk retrieves a chunk from disk and verifies its checksum
func (n *StorageNode) retrieveChunk(filename string, chunkNum int) ([]byte, error) {
	// Create chunk file path
	chunkPath := filepath.Join(n.dataDir, fmt.Sprintf("%s_%d", filename, chunkNum))
	
	// Open chunk file
	file, err := os.Open(chunkPath)
	if err != nil {
		return nil, &common.ChunkNotFoundError{
			Filename: filename,
			ChunkNum: chunkNum,
		}
	}
	defer file.Close()

	// Read stored checksum (32 bytes)
	var storedChecksum [32]byte
	if err := binary.Read(file, binary.LittleEndian, &storedChecksum); err != nil {
		return nil, fmt.Errorf("failed to read checksum: %v", err)
	}

	// Read data
	data, err := os.ReadFile(chunkPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk data: %v", err)
	}

	// Skip the checksum bytes from the data
	data = data[32:]

	// Verify checksum
	if !common.VerifyChecksum(data, storedChecksum[:]) {
		return nil, &common.ChunkCorruptionError{
			Filename: filename,
			ChunkNum: chunkNum,
		}
	}

	// Update statistics
	n.mu.Lock()
	n.requestsHandled++
	n.mu.Unlock()

	return data, nil
}

// Note: loadMetadata and saveMetadata are implemented in proto_handler.go

func main() {
	// Parse command line arguments
	nodeID := flag.String("id", "", "Node ID (port number)")
	controllerAddr := flag.String("controller", "localhost:8000", "Controller address")
	dataDir := flag.String("data", "", "Data directory path")
	reportExisting := flag.Bool("report-existing", false, "Whether to report existing files on startup")
	flag.Parse()

	// Validate arguments
	if *nodeID == "" || *dataDir == "" {
		log.Fatal("Node ID and data directory are required")
	}

	// Create and start storage node
	node := NewStorageNode(*nodeID, *controllerAddr, *dataDir, *reportExisting)
	if err := node.Start(); err != nil {
		log.Fatalf("Storage node failed to start: %v", err)
	}
}