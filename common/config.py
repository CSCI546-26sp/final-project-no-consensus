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

# Replication
REPLICATION_FACTOR = 2

# Heartbeat
HEARTBEAT_INTERVAL = 5      # seconds between heartbeats
HEARTBEAT_MISS_LIMIT = 3    # missed heartbeats before marking dead

#Garbage collection
GC_INTERVAL = 60
GC_THRESHOLD = 600

STOP_WORDS = {"the", "a", "an", "is", "are", "was", "were", "in", "on",
                "at", "to", "for", "of", "and", "or", "but", "not", "with",
                "this", "that", "it", "be", "as", "by", "from", "has", "had",
                "have", "will", "would", "could", "should", "may", "can"}
