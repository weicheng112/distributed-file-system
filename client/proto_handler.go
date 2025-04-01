package main

import (
	"fmt"
	"log"
	"net"
	"strings"

	"dfs/common"
	pb "dfs/proto"

	"google.golang.org/protobuf/proto"
)

// We're now using the Protocol Buffer generated types from dfs/proto

// getStorageLocations requests chunk storage locations from the controller
func (c *Client) getStorageLocations(filename string, fileSize int64, chunkSize int64) (map[int][]string, error) {
	// Connect to controller
	conn, err := net.Dial("tcp", c.controllerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer conn.Close()

	// Create request
	request := &pb.StorageRequest{
		Filename:  filename,
		FileSize:  uint64(fileSize),
		ChunkSize: uint32(chunkSize),
	}

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeStorageRequest, requestData); err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	if msgType != common.MsgTypeStorageResponse {
		return nil, fmt.Errorf("unexpected response type: %d", msgType)
	}

	response := &pb.StorageResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if response.Error != "" {
		return nil, fmt.Errorf("controller error: %s", response.Error)
	}

	// Convert response to map
	locations := make(map[int][]string)
	for _, placement := range response.ChunkPlacements {
		locations[int(placement.ChunkNumber)] = placement.StorageNodes
	}

	return locations, nil
}

// storeChunk stores a chunk on a storage node
func (c *Client) storeChunk(filename string, chunkNum int, data []byte, nodes []string) error {
	if len(nodes) == 0 {
		return fmt.Errorf("no storage nodes available for chunk")
	}

	// Connect to primary storage node
	// Ensure the node address includes the hostname
	nodeAddr := nodes[0]
	log.Printf("Node Addr:  %s", nodeAddr)
	// The controller should now provide the full address (hostname:port)
	// If it doesn't contain a colon, fall back to the old behavior
	if !strings.Contains(nodeAddr, ":") {
		log.Printf("Warning: Node address %s does not contain a hostname, falling back to localhost", nodeAddr)
		nodeAddr = "localhost:" + nodeAddr
	}
	
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to storage node: %v", err)
	}
	defer conn.Close()

	// Create request
	request := &pb.ChunkStoreRequest{
		Filename:     filename,
		ChunkNumber:  uint32(chunkNum),
		Data:         data,
		ReplicaNodes: nodes[1:], // Remaining nodes for replication
	}

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeChunkStore, requestData); err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	if msgType != common.MsgTypeChunkStore {
		return fmt.Errorf("unexpected response type: %d", msgType)
	}

	response := &pb.ChunkStoreResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("storage node error: %s", response.Error)
	}

	return nil
}

// getChunkLocations requests chunk locations from the controller
func (c *Client) getChunkLocations(filename string) (map[int][]string, error) {
	// Connect to controller
	conn, err := net.Dial("tcp", c.controllerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer conn.Close()

	// Create request
	request := &pb.RetrievalRequest{
		Filename: filename,
	}

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeRetrievalRequest, requestData); err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	if msgType != common.MsgTypeRetrievalResponse {
		return nil, fmt.Errorf("unexpected response type: %d", msgType)
	}

	response := &pb.RetrievalResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if response.Error != "" {
		return nil, fmt.Errorf("controller error: %s", response.Error)
	}

	// Convert response to map
	locations := make(map[int][]string)
	for _, chunk := range response.Chunks {
		locations[int(chunk.ChunkNumber)] = chunk.StorageNodes
	}

	return locations, nil
}

// retrieveChunk retrieves a chunk from a storage node
func (c *Client) retrieveChunk(filename string, chunkNum int, nodes []string) ([]byte, error) {
	// Try each node until successful
	var lastErr error
	for _, node := range nodes {
		data, err := c.retrieveChunkFromNode(filename, chunkNum, node)
		if err == nil {
			return data, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("failed to retrieve chunk from all nodes: %v", lastErr)
}

// retrieveChunkFromNode retrieves a chunk from a specific storage node
func (c *Client) retrieveChunkFromNode(filename string, chunkNum int, node string) ([]byte, error) {
	// Connect to storage node
	// Ensure the node address includes the hostname
	nodeAddr := node
	// The controller should now provide the full address (hostname:port)
	// If it doesn't contain a colon, fall back to the old behavior
	if !strings.Contains(nodeAddr, ":") {
		log.Printf("Warning: Node address %s does not contain a hostname, falling back to localhost", nodeAddr)
		nodeAddr = "localhost:" + nodeAddr
	}
	
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to storage node: %v", err)
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
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeChunkRetrieve, requestData); err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	if msgType != common.MsgTypeChunkRetrieve {
		return nil, fmt.Errorf("unexpected response type: %d", msgType)
	}

	response := &pb.ChunkRetrieveResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if response.Error != "" {
		return nil, fmt.Errorf("storage node error: %s", response.Error)
	}

	return response.Data, nil
}

// requestFileDeletion requests the controller to delete a file
func (c *Client) requestFileDeletion(filename string) error {
	// Connect to controller
	conn, err := net.Dial("tcp", c.controllerAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer conn.Close()

	// Create request
	request := &pb.DeleteRequest{
		Filename: filename,
	}

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeDeleteRequest, requestData); err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	if msgType != common.MsgTypeDeleteResponse {
		return fmt.Errorf("unexpected response type: %d", msgType)
	}

	response := &pb.DeleteResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("controller error: %s", response.Error)
	}

	return nil
}

// requestFileList requests a list of files from the controller
func (c *Client) requestFileList() ([]*pb.FileInfo, error) {
	// Connect to controller
	conn, err := net.Dial("tcp", c.controllerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer conn.Close()

	// Create empty request
	request := &pb.ListFilesRequest{}

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeListRequest, requestData); err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	if msgType != common.MsgTypeListResponse {
		return nil, fmt.Errorf("unexpected response type: %d", msgType)
	}

	response := &pb.ListFilesResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return response.Files, nil
}

// requestNodeStatus requests the status of all nodes from the controller
func (c *Client) requestNodeStatus() (*pb.NodeStatusResponse, error) {
	// Connect to controller
	conn, err := net.Dial("tcp", c.controllerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer conn.Close()

	// Create empty request
	request := &pb.NodeStatusRequest{}

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeNodeStatusRequest, requestData); err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	// The controller responds with the corresponding response message type
	if msgType != common.MsgTypeNodeStatusResponse {
		return nil, fmt.Errorf("unexpected response type: %d", msgType)
	}

	response := &pb.NodeStatusResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return response, nil
}