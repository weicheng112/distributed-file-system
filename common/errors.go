package common

import "fmt"

// ChunkCorruptionError is raised when a chunk's checksum verification fails
type ChunkCorruptionError struct {
	Filename string
	ChunkNum int
}

func (e ChunkCorruptionError) Error() string {
	return fmt.Sprintf("chunk %s_%d is corrupted", e.Filename, e.ChunkNum)
}

// FileNotFoundError is raised when a requested file doesn't exist
type FileNotFoundError struct {
	Filename string
}

func (e FileNotFoundError) Error() string {
	return fmt.Sprintf("file %s not found", e.Filename)
}

// ChunkNotFoundError is raised when a specific chunk cannot be located
type ChunkNotFoundError struct {
	Filename string
	ChunkNum int
}

func (e ChunkNotFoundError) Error() string {
	return fmt.Sprintf("chunk %s_%d not found", e.Filename, e.ChunkNum)
}

// ProtocolError is raised when there's an issue with message formatting or parsing
type ProtocolError struct {
	Message string
}

func (e ProtocolError) Error() string {
	return fmt.Sprintf("protocol error: %s", e.Message)
}