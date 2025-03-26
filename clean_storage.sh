#!/bin/bash

# Simple script to remove all storage node data

echo "Removing all storage node data..."

# Remove all files in data directories
rm -f storage/data1/*
rm -f storage/data2/*
rm -f storage/data3/*

echo "Storage data removed successfully."