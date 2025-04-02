package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"dfs/common"
	pb "dfs/proto"
	"google.golang.org/protobuf/proto"
)

// FileMetadata stores information about a file in the system
type FileMetadata struct {
	Size      int64             // Total size of the file in bytes
	ChunkSize int               // Size of each chunk in bytes
	Chunks    map[int][]string  // Map of chunk number to list of storage nodes
}

// NodeInfo stores information about a storage node
type NodeInfo struct {
	ID               string            // Unique identifier for the node (typically IP:port)
	Address          string            // Network address of the node
	FreeSpace        uint64            // Available storage space in bytes
	RequestsHandled  uint64            // Number of requests processed by this node
	LastHeartbeat    time.Time         // Timestamp of the last heartbeat received
	ReplicatedChunks map[string][]int  // Map of filename to chunk numbers stored on this node
}

// Controller manages the distributed file system metadata and coordinates storage nodes
type Controller struct {
	mu sync.RWMutex

	// Map of active storage nodes
	// Key: node ID (ip:port), Value: node information
	nodes map[string]*NodeInfo

	// Map of files to their metadata
	// Key: filename, Value: file metadata
	files map[string]*FileMetadata

	// Configuration
	replicationFactor int           // Number of replicas for each chunk
	heartbeatTimeout  time.Duration // Time after which a node is considered down

	// Listener for incoming connections
	listener net.Listener

	// Port number
	port int
}

// NewController creates a new controller instance
func NewController(listenPort int) *Controller {
	return &Controller{
		nodes:             make(map[string]*NodeInfo),
		files:             make(map[string]*FileMetadata),
		replicationFactor: common.DefaultReplication,
		heartbeatTimeout:  time.Duration(common.HeartbeatTimeout) * time.Second,
		port:              listenPort,
	}
}

// Start initializes the controller and begins listening for connections
func (c *Controller) Start() error {
	// Start listening for connections
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", c.port))
	if err != nil {
		return fmt.Errorf("failed to start listener: %v", err)
	}
	c.listener = listener

	// Start background tasks
	go c.checkNodeHealth()
	go c.maintainReplication()

	log.Printf("Controller started on port %d", c.port)

	// Accept and handle connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go c.handleConnection(conn)
	}
}

// handleConnection processes incoming connections from clients and storage nodes
func (c *Controller) handleConnection(conn net.Conn) {
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
		case common.MsgTypeHeartbeat:
			respErr = c.handleHeartbeat(data)
		case common.MsgTypeStorageRequest:
			response, respErr = c.handleStorageRequest(data)
		case common.MsgTypeRetrievalRequest:
			response, respErr = c.handleRetrievalRequest(data)
		case common.MsgTypeDeleteRequest:
			response, respErr = c.handleDeleteRequest(data)
		case common.MsgTypeListRequest:
			response, respErr = c.handleListRequest(data)
		case common.MsgTypeNodeStatusRequest:
			response, respErr = c.handleNodeStatusRequest(data)
		default:
			respErr = fmt.Errorf("unknown message type: %d", msgType)
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
			// Map request message types to their corresponding response types
			var responseType byte
			switch msgType {
			case common.MsgTypeStorageRequest:
				responseType = common.MsgTypeStorageResponse
			case common.MsgTypeRetrievalRequest:
				responseType = common.MsgTypeRetrievalResponse
			case common.MsgTypeDeleteRequest:
				responseType = common.MsgTypeDeleteResponse
			case common.MsgTypeListRequest:
				responseType = common.MsgTypeListResponse
			case common.MsgTypeNodeStatusRequest:
				responseType = common.MsgTypeNodeStatusResponse
			default:
				responseType = msgType
			}
			
			if err := common.WriteMessage(conn, responseType, response); err != nil {
				log.Printf("Error sending response: %v", err)
				return
			}
		}
	}
}

// checkNodeHealth periodically checks if nodes are still alive
// If a node hasn't sent a heartbeat within the timeout period, it's considered down
func (c *Controller) checkNodeHealth() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for nodeID, info := range c.nodes {
			if now.Sub(info.LastHeartbeat) > c.heartbeatTimeout {
				log.Printf("Node %s appears to be down, removing from active nodes", nodeID)
				delete(c.nodes, nodeID)
				go c.handleNodeFailure(nodeID)
			}
		}
		c.mu.Unlock()
	}
}

// handleNodeFailure processes the failure of a storage node
// It identifies affected chunks and triggers re-replication
func (c *Controller) handleNodeFailure(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("Processing failure of node %s", nodeID)

	// Get the node's address and replicated chunks if it exists
	failedNodeInfo, exists := c.nodes[nodeID]
	var failedNodeAddr string
	if exists {
		failedNodeAddr = failedNodeInfo.Address
		log.Printf("Failed node address: %s", failedNodeAddr)
		
		// Log the chunks that were stored on this node according to the node's ReplicatedChunks map
		log.Printf("Chunks stored on node %s according to its ReplicatedChunks map:", nodeID)
		for filename, chunks := range failedNodeInfo.ReplicatedChunks {
			log.Printf("  File: %s, Chunks: %v", filename, chunks)
		}
	} else {
		// If the node info doesn't exist, use the nodeID as the address
		failedNodeAddr = nodeID
		log.Printf("Failed node address: %s (node info not found)", failedNodeAddr)
	}

	// Find all chunks that were stored on the failed node
	affectedChunks := make(map[string][]int) // filename -> chunk numbers
	
	// First, check the node's ReplicatedChunks map if it exists
	if exists && failedNodeInfo.ReplicatedChunks != nil {
		for filename, chunks := range failedNodeInfo.ReplicatedChunks {
			affectedChunks[filename] = chunks
			log.Printf("Adding chunks %v of file %s to affected chunks from ReplicatedChunks", chunks, filename)
		}
	}
	
	// Then, check the file metadata
	log.Printf("Checking file metadata for chunks stored on node %s or %s", nodeID, failedNodeAddr)
	for filename, metadata := range c.files {
		log.Printf("Checking file %s", filename)
		for chunkNum, nodes := range metadata.Chunks {
			log.Printf("  Chunk %d is stored on nodes: %v", chunkNum, nodes)
			for i, node := range nodes {
				// Match against the node ID, the node address, or if the address contains the node ID
				if node == nodeID || node == failedNodeAddr || strings.Contains(node, ":"+nodeID) {
					// Remove the failed node from the list
					log.Printf("  Removing node %s from chunk %d of file %s", node, chunkNum, filename)
					metadata.Chunks[chunkNum] = append(nodes[:i], nodes[i+1:]...)
					
					// Add to affected chunks if not already there
					found := false
					for _, c := range affectedChunks[filename] {
						if c == chunkNum {
							found = true
							break
						}
					}
					if !found {
						affectedChunks[filename] = append(affectedChunks[filename], chunkNum)
						log.Printf("  Adding chunk %d of file %s to affected chunks", chunkNum, filename)
					}
					break
				}
			}
		}
	}

	// Trigger re-replication for affected chunks
	log.Printf("Node %s was responsible for %d chunks across %d files",
		nodeID, countTotalChunks(affectedChunks), len(affectedChunks))
	for filename, chunks := range affectedChunks {
		for _, chunkNum := range chunks {
			log.Printf("Triggering re-replication for chunk %d of file %s", chunkNum, filename)
			go c.replicateChunk(filename, chunkNum)
		}
	}
}

// countTotalChunks counts the total number of chunks in the affected chunks map
func countTotalChunks(affectedChunks map[string][]int) int {
	count := 0
	for _, chunks := range affectedChunks {
		count += len(chunks)
	}
	return count
}

// maintainReplication periodically checks if all chunks have the required number of replicas
func (c *Controller) maintainReplication() {
	// Reduced from 1 minute to 10 seconds for faster recovery during testing
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		c.mu.RLock()
		// Check replication level of all chunks
		for filename, metadata := range c.files {
			for chunkNum, nodes := range metadata.Chunks {
				if len(nodes) < c.replicationFactor {
					log.Printf("Replication check: file %s chunk %d has %d replicas, need %d",
						filename, chunkNum, len(nodes), c.replicationFactor)
					go c.replicateChunk(filename, chunkNum)
				}
			}
		}
		c.mu.RUnlock()
	}
}

// replicateChunk ensures a chunk has the required number of replicas
// This is called when a node fails or when the replication level is below the threshold
func (c *Controller) replicateChunk(filename string, chunkNum int) {
	c.mu.Lock()
	metadata, exists := c.files[filename]
	if !exists {
		c.mu.Unlock()
		log.Printf("Cannot replicate chunk: file %s not found", filename)
		return
	}
	
	// Get current nodes that have this chunk
	currentNodes, exists := metadata.Chunks[chunkNum]
	if !exists {
		c.mu.Unlock()
		log.Printf("Cannot replicate chunk: chunk %d of file %s not found", chunkNum, filename)
		return
	}
	
	// If we already have enough replicas, no need to replicate
	if len(currentNodes) >= c.replicationFactor {
		c.mu.Unlock()
		log.Printf("Chunk %d of file %s already has sufficient replicas (%d)",
			chunkNum, filename, len(currentNodes))
		return
	}
	
	log.Printf("Replicating chunk %d of file %s - current replicas: %d, target: %d",
		chunkNum, filename, len(currentNodes), c.replicationFactor)
	
	// Select new nodes for replication
	neededReplicas := c.replicationFactor - len(currentNodes)
	log.Printf("Need %d additional replicas for chunk %d of file %s",
		neededReplicas, chunkNum, filename)
	
	// Get all available nodes
	availableNodes := make([]string, 0)
	for nodeID, nodeInfo := range c.nodes {
		// Check if this node already has the chunk
		hasChunk := false
		for _, existingNode := range currentNodes {
			// Match against the node ID, the node address, or if the address contains the node ID
			if nodeID == existingNode || nodeInfo.Address == existingNode || strings.Contains(existingNode, ":"+nodeID) {
				hasChunk = true
				break
			}
		}
		if !hasChunk {
			availableNodes = append(availableNodes, nodeID)
		}
	}
	
	if len(availableNodes) == 0 {
		c.mu.Unlock()
		log.Printf("Cannot replicate chunk: no available nodes")
		return
	}
	
	log.Printf("Found %d available nodes for replication", len(availableNodes))
	
	// Select nodes for replication (up to the number needed)
	selectedNodes := availableNodes
	if len(selectedNodes) > neededReplicas {
		selectedNodes = selectedNodes[:neededReplicas]
	}
	
	// Get a source node that has the chunk
	if len(currentNodes) == 0 {
		c.mu.Unlock()
		log.Printf("Cannot replicate chunk: no source nodes available")
		return
	}
	sourceNode := currentNodes[0]
	
	// Update metadata with new nodes
	for _, nodeID := range selectedNodes {
		// Get the full address from the NodeInfo object
		nodeInfo := c.nodes[nodeID]
		metadata.Chunks[chunkNum] = append(metadata.Chunks[chunkNum], nodeInfo.Address)
		log.Printf("Added node %s (Address: %s) as a replica for chunk %d of file %s",
			nodeID, nodeInfo.Address, chunkNum, filename)
	}
	c.mu.Unlock()
	
	log.Printf("Replicating chunk %d of file %s from %s to %v",
		chunkNum, filename, sourceNode, selectedNodes)
	
	// Coordinate replication by instructing the source node to send the chunk to new nodes
	// In a production system, we would implement the actual coordination here
	// For this implementation, we'll simulate the coordination by:
	// 1. Retrieving the chunk from the source node
	// 2. Sending it to each of the new nodes
	
	// Connect to source node
	// Get the full address from the NodeInfo object
	c.mu.RLock()
	sourceNodeInfo, exists := c.nodes[sourceNode]
	var sourceAddr string
	if exists {
		sourceAddr = sourceNodeInfo.Address
	} else {
		// If the node info doesn't exist (which shouldn't happen), fall back to the old behavior
		sourceAddr = sourceNode
		if !strings.Contains(sourceAddr, ":") {
			sourceAddr = "localhost:" + sourceAddr
		}
	}
	c.mu.RUnlock()
	
	conn, err := net.Dial("tcp", sourceAddr)
	if err != nil {
		log.Printf("Failed to connect to source node %s: %v", sourceNode, err)
		return
	}
	defer conn.Close()
	
	// Create retrieve request
	request := &pb.ChunkRetrieveRequest{
		Filename:    filename,
		ChunkNumber: uint32(chunkNum),
	}
	
	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		log.Printf("Failed to marshal retrieve request: %v", err)
		return
	}
	
	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeChunkRetrieve, requestData); err != nil {
		log.Printf("Failed to send retrieve request: %v", err)
		return
	}
	
	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		log.Printf("Failed to read retrieve response: %v", err)
		return
	}
	
	if msgType != common.MsgTypeChunkRetrieve {
		log.Printf("Unexpected response type from source node: %d", msgType)
		return
	}
	
	response := &pb.ChunkRetrieveResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		log.Printf("Failed to unmarshal retrieve response: %v", err)
		return
	}
	
	// Now send the chunk to each new node
	for _, newNode := range selectedNodes {
		go func(node string) {
			// Connect to new node
			// Get the full address from the NodeInfo object
			c.mu.RLock()
			nodeInfo, exists := c.nodes[node]
			var nodeAddr string
			if exists {
				nodeAddr = nodeInfo.Address
			} else {
				// If the node info doesn't exist (which shouldn't happen), fall back to the old behavior
				nodeAddr = node
				if !strings.Contains(nodeAddr, ":") {
					nodeAddr = "localhost:" + nodeAddr
				}
			}
			c.mu.RUnlock()
			
			conn, err := net.Dial("tcp", nodeAddr)
			if err != nil {
				log.Printf("Failed to connect to new node %s: %v", node, err)
				return
			}
			defer conn.Close()
			
			// Create store request
			storeRequest := &pb.ChunkStoreRequest{
				Filename:     filename,
				ChunkNumber:  uint32(chunkNum),
				Data:         response.Data,
				ReplicaNodes: []string{}, // No further replication needed
			}
			
			// Serialize request
			storeRequestData, err := proto.Marshal(storeRequest)
			if err != nil {
				log.Printf("Failed to marshal store request: %v", err)
				return
			}
			
			// Send request
			if err := common.WriteMessage(conn, common.MsgTypeChunkStore, storeRequestData); err != nil {
				log.Printf("Failed to send store request: %v", err)
				return
			}
			
			// Read response
			msgType, storeResponseData, err := common.ReadMessage(conn)
			if err != nil {
				log.Printf("Failed to read store response: %v", err)
				return
			}
			
			if msgType != common.MsgTypeChunkStore {
				log.Printf("Unexpected response type from new node: %d", msgType)
				return
			}
			
			storeResponse := &pb.ChunkStoreResponse{}
			if err := proto.Unmarshal(storeResponseData, storeResponse); err != nil {
				log.Printf("Failed to unmarshal store response: %v", err)
				return
			}
			
			if !storeResponse.Success {
				log.Printf("Failed to store chunk on new node %s: %s", node, storeResponse.Error)
				return
			}
			
			log.Printf("Successfully replicated chunk %d of file %s to node %s", chunkNum, filename, node)
		}(newNode)
	}
}


func main() {
	listenPort := flag.Int("port", 8000, "Port to listen on")
	flag.Parse()

	controller := NewController(*listenPort)
	if err := controller.Start(); err != nil {
		log.Fatalf("Controller failed to start: %v", err)
	}
}