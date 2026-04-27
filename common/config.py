# Master
MASTER_HOST = "dfs-master"
MASTER_PORT = 5050

# Chunk servers
CHUNK_SERVER_PORTS = {
    1: 6001,
    2: 6002,
    3: 6003,
}

# Chunk settings
CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB in bytes
PIPELINE_QUEUE_MAXSIZE = 16

# gRPC message size cap. Default is 4 MB; raised to 40 MB for aggregated search
# responses (FileSearch fans out across many chunks, response can grow large).
GRPC_MAX_MESSAGE_BYTES = 40 * 1024 * 1024
GRPC_OPTIONS = [
    ("grpc.max_send_message_length", GRPC_MAX_MESSAGE_BYTES),
    ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_BYTES),
]

# Replication
REPLICATION_FACTOR = 1

# Heartbeat
HEARTBEAT_INTERVAL = 5      # seconds between heartbeats
HEARTBEAT_MISS_LIMIT = 3    # missed heartbeats before marking dead

#Garbage collection
GC_INTERVAL = 60
GC_THRESHOLD = 600

#Indexing parallelism
INDEXER_WORKERS = 4

STOP_WORDS = {"the", "a", "an", "is", "are", "was", "were", "in", "on",
                "at", "to", "for", "of", "and", "or", "but", "not", "with",
                "this", "that", "it", "be", "as", "by", "from", "has", "had",
                "have", "will", "would", "could", "should", "may", "can"}
