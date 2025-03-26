package common

// Message types for protocol
const (
	MsgTypeHeartbeat        byte = 1
	MsgTypeStorageRequest   byte = 2
	MsgTypeStorageResponse  byte = 3
	MsgTypeRetrievalRequest byte = 4
	MsgTypeRetrievalResponse byte = 5
	MsgTypeDeleteRequest    byte = 6
	MsgTypeDeleteResponse   byte = 7
	MsgTypeListRequest      byte = 8
	MsgTypeListResponse     byte = 9
	MsgTypeNodeStatusRequest byte = 10
	MsgTypeNodeStatusResponse byte = 11
	MsgTypeChunkStore       byte = 12
	MsgTypeChunkRetrieve    byte = 13
)

// Default values
const (
	DefaultChunkSize    = 64 * 1024 * 1024 // 64MB
	DefaultReplication  = 3
	HeartbeatInterval  = 5  // seconds
	HeartbeatTimeout   = 15 // seconds
)