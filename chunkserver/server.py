from typing import Iterator
import argparse
from concurrent.futures import ThreadPoolExecutor
import threading
import grpc
import time

from chunkserver.index import InvertedIndex
from chunkserver.store import ChunkStore

from common.config import HEARTBEAT_INTERVAL, MASTER_HOST, MASTER_PORT

from proto.chunkserver_pb2_grpc import ChunkServerServiceServicer, add_ChunkServerServiceServicer_to_server
from proto.heartbeat_pb2_grpc import HeartbeatServiceServicer, HeartbeatServiceStub, add_HeartbeatServiceServicer_to_server
from proto.heartbeat_pb2 import HeartbeatRequest, SearchChunksResponse, SearchChunksRequest, ChunkMatch
from proto.chunkserver_pb2 import WriteChunkResponse, WriteChunkRequest, ReadChunkRequest, ReadChunkResponse


class ChunkServer(ChunkServerServiceServicer, HeartbeatServiceServicer):
    
    serverId: int
    port: int
    dataDir: str
    store: ChunkStore
    index: InvertedIndex

    def __init__(self, serverId: int, port: int, dataDir: str):
        self.serverId = serverId
        self.port = port
        self.store = ChunkStore(dataDir)
        self.index = InvertedIndex()
        self.lock = threading.Lock()

        for chunkHandle in self.store.listChunks():
            data = self.store.readChunk(chunkHandle)
            self.index.indexChunk(chunkHandle, data.decode("utf-8"))

        threading.Thread(target= self._sendHeartbeats, daemon=True).start()
        return
    
    def _sendHeartbeats(self):
        channel = grpc.insecure_channel(f"{MASTER_HOST}:{MASTER_PORT}")
        stub = HeartbeatServiceStub(channel)
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            try:
                request = HeartbeatRequest(
                    server_id = self.serverId,
                    available_disk = self.store.getAvailableDisk(),
                    chunk_handles = self.store.listChunks()
                )
                stub.Heartbeat(request)
                print(f"Heartbeat sent from server {self.serverId}")

            except grpc.RpcError as e:
                print(f"Heartbeat failed from server {self.serverId} : {e}")
                
    
    def WriteChunk(self, 
                   request_iterator: Iterator[WriteChunkRequest], 
                   context: grpc.ServicerContext) -> WriteChunkResponse:
        data = bytearray()
        chunkHandle = None
        forwardAddresses = []

        for request in request_iterator:
            if chunkHandle is None:
                chunkHandle = request.chunk_handle
                forwardAddresses = list(request.forward_addresses)
            data.extend(request.data)
        data = bytes(data)

        with self.lock:
            self.store.writeChunk(chunkHandle = chunkHandle, data = data)
            self.index.indexChunk(chunkHandle = chunkHandle, text = data.decode("utf-8"))

        #TODO: forward to replicas

        return WriteChunkResponse(success = True, message = "Chunk Written")
    
    def ReadChunk(self, request: ReadChunkRequest, 
                  context: grpc.ServicerContext) -> Iterator[ReadChunkResponse]:
        chunkHandle = request.chunk_handle
        with self.lock:
            if not self.store.chunkExists(chunkHandle = chunkHandle):
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Chunk does not exist")
                return 

            data = self.store.readChunk(chunkHandle = chunkHandle)
        
        #Yield outside lock to ensure no blockages
        #Stream size will be 512 KB pieces
        STREAM_CHUNK_SIZE = 512 * 1024
        for i in range(0, len(data), STREAM_CHUNK_SIZE):
            yield ReadChunkResponse(data = data[i: i + STREAM_CHUNK_SIZE])

        return
    
    def SearchChunks(self, 
                     request: SearchChunksRequest, 
                     context: grpc.ServicerContext) -> SearchChunksResponse:
        with self.lock:
            results = self.index.search(request.query)

        matches = []
        for (chunkHandle, [score, lineNumber, lineText]) in results:
            matches.append(ChunkMatch(
                chunk_handle = chunkHandle,
                line_number = lineNumber,
                score = score,
                snippet = lineText
                ))
        return SearchChunksResponse(matches = matches)

def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type = int, required = True)
    parser.add_argument("--port", type = int, required = True)
    args = parser.parse_args()

    dataDir = f"/data/chunk{args.id}"
    chunkServer = ChunkServer(serverId = args.id, port = args.port, dataDir = dataDir)

    server = grpc.server(ThreadPoolExecutor(max_workers = 10))
    add_ChunkServerServiceServicer_to_server(chunkServer, server)
    add_HeartbeatServiceServicer_to_server(chunkServer, server)

    server.add_insecure_port(f"0.0.0.0:{args.port}")
    server.start()
    print(f"Chunk server {args.id} started on port {args.port}")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()