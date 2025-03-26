# Makefile for Distributed File System

.PHONY: all proto clean

all: proto

# Generate Go code from protocol buffer definitions
proto:
	protoc --go_out=. --go_opt=paths=source_relative proto/dfs.proto

# Clean generated files
clean:
	rm -f proto/*.pb.go