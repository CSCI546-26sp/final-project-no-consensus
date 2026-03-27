# Distributed File System with Integrated Full-Text Search

A GFS-inspired distributed file system with built-in inverted index search.

## Setup

### 1. Create conda environment
```bash
conda create -n dfs python=3.11 -y
conda activate dfs
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Generate gRPC code from proto files
```bash
python -m grpc_tools.protoc -I proto --python_out=proto --grpc_python_out=proto proto/master.proto proto/chunkserver.proto proto/heartbeat.proto
```
This must be run after cloning the repo and whenever `.proto` files are modified.

### 4. Start the cluster
```bash
cd docker && docker compose up --build
```

To stop:
```bash
docker compose down
```

## Project Structure
```
master/          - Master node (metadata, coordination, search merging)
chunkserver/     - Chunk servers (storage, indexing, local search)
client/          - Client CLI (upload, download, search)
common/          - Shared config and utilities
proto/           - gRPC service definitions (.proto files)
docker/          - Dockerfile and docker-compose.yml
tests/           - Tests
```
