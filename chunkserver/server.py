from collections import deque
from typing import Iterator
import argparse
from concurrent.futures import ThreadPoolExecutor
import threading
import grpc
import time
import re

from chunkserver.index import InvertedIndex
from chunkserver.store import ChunkStore

from common.config import HEARTBEAT_INTERVAL, MASTER_HOST, MASTER_PORT

from proto.chunkserver_pb2_grpc import ChunkServerServiceServicer, add_ChunkServerServiceServicer_to_server, ChunkServerServiceStub
from proto.heartbeat_pb2_grpc import HeartbeatServiceServicer, HeartbeatServiceStub, add_HeartbeatServiceServicer_to_server

from proto.heartbeat_pb2 import HeartbeatRequest, SearchChunksResponse, SearchChunksRequest, ChunkMatch
from proto.heartbeat_pb2 import ReplicateChunkRequest, ReplicateChunkResponse
from proto.heartbeat_pb2 import ScanChunkRequest, ScanChunkResponse, ScanMatch
from proto.heartbeat_pb2 import DeleteChunkRequest, DeleteChunkResponse

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
            self.index.indexChunk(chunkHandle, data.decode("utf-8", errors="replace"))

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
                    chunk_handles = self.store.listChunks(),
                    used_bytes = self.store.getUsedBytes()
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
                forwardAddresses = deque(request.forward_addresses)
            data.extend(request.data)
        data = bytes(data)

        with self.lock:
            self.store.writeChunk(chunkHandle = chunkHandle, data = data)
            self.index.indexChunk(chunkHandle = chunkHandle, text = data.decode("utf-8", errors="replace"))

        if len(forwardAddresses):
            nextAddress = forwardAddresses.popleft()
            try:
                channel = grpc.insecure_channel(nextAddress)
                stub = ChunkServerServiceStub(channel)
                STREAM_SIZE = 512 * 1024
                def forwardIterator():
                    for i in range(0, len(data), STREAM_SIZE):
                        yield WriteChunkRequest(
                            chunk_handle = chunkHandle,
                            data = data[i: i+STREAM_SIZE],
                            forward_addresses = forwardAddresses
                        )
                response = stub.WriteChunk(forwardIterator())
                if not response.success:
                    return WriteChunkResponse(success = False, message = f"Replica Forwarding failed")
                
            except grpc.RpcError as rpcError:
                return WriteChunkResponse(success = False, message = f"Failed to open connection to replication servers: {rpcError.details()}")            

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
        TOP_K = 10
        with self.lock:
            results = self.index.search(request.query)[:TOP_K]

        matches = []
        for (chunkHandle, score, lineNumber) in results:
            lineText = ""
            try:
                data = self.store.readChunk(chunkHandle)
                lines = data.decode("utf-8", errors="replace").split("\n")
                if 0 <= lineNumber < len(lines):
                    lineText = lines[lineNumber]
            except FileNotFoundError:
                pass
            matches.append(ChunkMatch(
                chunk_handle = chunkHandle,
                line_number = lineNumber,
                score = score,
                snippet = lineText,
            ))
        return SearchChunksResponse(matches = matches)
    
    def ReplicateChunk(self, 
                       request: ReplicateChunkRequest, 
                       context: grpc.ServicerContext) -> ReplicateChunkResponse:
        try:
            channel = grpc.insecure_channel(request.source_address)
            stub = ChunkServerServiceStub(channel)
            data = bytearray()
            for response in stub.ReadChunk(ReadChunkRequest(chunk_handle = request.chunk_handle)):
                data.extend(response.data)
            data = bytes(data)

            with self.lock:
                self.store.writeChunk(chunkHandle = request.chunk_handle, data = data)
                self.index.indexChunk(chunkHandle = request.chunk_handle, text = data.decode("utf-8", errors="replace"))
            return ReplicateChunkResponse(success = True, message = "Chunk Replicated")
        except grpc.RpcError as rpcError:
            return ReplicateChunkResponse(success = False, message = rpcError.details())
        
    def ScanChunk(self, 
                  request: ScanChunkRequest, 
                  context: grpc.ServicerContext) -> ScanChunkResponse:
        chunkHandle = request.chunk_handle
        query = request.query
        if not query:
            return ScanChunkResponse()

        with self.lock:
            if not self.store.chunkExists(chunkHandle = chunkHandle):
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Chunk does not exist")
                return ScanChunkResponse()
            
            data = self.store.readChunk(chunkHandle = chunkHandle)

        text = data.decode("utf-8", errors = "replace")
        pattern = re.compile(re.escape(query), re.IGNORECASE)

        matches = []
        for lineNumber, line in enumerate(text.splitlines()):
            if pattern.search(line):
                matches.append(ScanMatch(
                    line_number = lineNumber,
                    line_text = line,
                ))
        return ScanChunkResponse(matches = matches)

    def DeleteChunk(self,
                    request: DeleteChunkRequest,
                    context: grpc.ServicerContext) -> DeleteChunkResponse:
        chunkHandle = request.chunk_handle
        with self.lock:
            if not self.store.chunkExists(chunkHandle = chunkHandle):
                return DeleteChunkResponse(success = True, message = "Chunk not present")
            self.store.deleteChunk(chunkHandle = chunkHandle)
            self.index.removeChunk(chunkHandle)
        return DeleteChunkResponse(success = True, message = "Chunk deleted")


def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type = int, required = True)
    parser.add_argument("--port", type = int, required = True)
    args = parser.parse_args()

    dataDir = f"/app/data"
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