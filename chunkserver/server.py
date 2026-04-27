from typing import Iterator
import argparse
import bisect
from concurrent.futures import ThreadPoolExecutor
import threading
import grpc
import time
import re
import queue

from chunkserver.index import InvertedIndex
from chunkserver.ngram import NGramIndex
from chunkserver.store import ChunkStore
from chunkserver.rwlock import RWLock

from common.config import HEARTBEAT_INTERVAL, MASTER_HOST, MASTER_PORT, PIPELINE_QUEUE_MAXSIZE, INDEXER_WORKERS, GRPC_OPTIONS

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
        self.ngramIndex = NGramIndex()
        self.lock = RWLock()

        self.indexQueue: queue.Queue = queue.Queue()
        for _ in range(INDEXER_WORKERS):
            threading.Thread(target=self._indexerLoop, daemon=True).start()

        # Enqueue existing chunks for async indexing; search may return partial                                                                                  
        # results until _indexerLoop drains the queue.
        for chunkHandle in self.store.listChunks():
            self.indexQueue.put(chunkHandle)

        threading.Thread(target= self._sendHeartbeats, daemon=True).start()
        return
    
    def _indexerLoop(self):
        # Shared between all worker threads — fan out the two tokenize calls
        # so the inverted-index regex (which releases the GIL) can overlap with
        # the ngram tokenize loop on the same chunk.
        tokenizePool = ThreadPoolExecutor(max_workers=2)
        while True:
            chunkHandle = self.indexQueue.get()
            if chunkHandle is None:
                return
            try:
                data = self.store.readChunk(chunkHandle)
                text = data.decode("utf-8", errors = "replace")
                invertedFuture = tokenizePool.submit(self.index.tokenizeChunk, chunkHandle, text)
                ngramFuture = tokenizePool.submit(self.ngramIndex.tokenizeChunk, chunkHandle, text)
                postings, lineBreaks = invertedFuture.result()
                ngramTrigrams = ngramFuture.result()
                with self.lock.writeLock():
                    self.index.mergeChunk(chunkHandle, postings, lineBreaks)
                    self.ngramIndex.mergeChunk(chunkHandle, ngramTrigrams)
            except Exception as ex:
                print(f"Indexer error for chunk {chunkHandle}: {ex}")
    
    def _sendHeartbeats(self):
        channel = grpc.insecure_channel(f"{MASTER_HOST}:{MASTER_PORT}", options=GRPC_OPTIONS)
        stub = HeartbeatServiceStub(channel)
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            try:
                with self.lock.readLock():
                    indexedHandles = list(self.index.chunkTerms.keys())
                request = HeartbeatRequest(
                    server_id = self.serverId,
                    available_disk = self.store.getAvailableDisk(),
                    chunk_handles = self.store.listChunks(),
                    used_bytes = self.store.getUsedBytes(),
                    indexed_chunk_handles = indexedHandles,
                )
                stub.Heartbeat(request)
                print(f"Heartbeat sent from server {self.serverId}")

            except grpc.RpcError as e:
                print(f"Heartbeat failed from server {self.serverId} : {e}")
                
    
    def WriteChunk(self, 
                   request_iterator: Iterator[WriteChunkRequest], 
                   context: grpc.ServicerContext) -> WriteChunkResponse:
        chunkHandle = None
        diskQueue: queue.Queue = queue.Queue(maxsize = PIPELINE_QUEUE_MAXSIZE)
        forwardQueue: queue.Queue = queue.Queue(maxsize = PIPELINE_QUEUE_MAXSIZE)
        diskResult = {"success": False, "message": ""}
        forwardResult = {"success": True, "message": ""}
        diskThread = None
        forwardThread = None

        def diskWorker(hanlde:str):
            try:
                path = self.store.getChunkPath(hanlde)
                with open(path, "wb") as f:
                    while True:
                        item = diskQueue.get()
                        if item is None:
                            break
                        f.write(item)
                diskResult["success"] = True
            except Exception as ex:
                diskResult["success"] = False
                diskResult["message"] = str(ex)
        
        def forwardWorker(nextAddress: str, handle: str, remainingForwards: list):
            try:
                channel = grpc.insecure_channel(nextAddress, options=GRPC_OPTIONS)
                stub = ChunkServerServiceStub(channel)
                def wIterator():
                    while True:
                        item = forwardQueue.get()
                        if item is None:
                            return
                        yield WriteChunkRequest(
                            chunk_handle = chunkHandle,
                            data = item,
                            forward_addresses = remainingForwards
                        )
                response = stub.WriteChunk(wIterator())
                forwardResult["success"] = response.success
                forwardResult["message"] = response.message

            except grpc.RpcError as e:
                forwardResult["success"] = False
                forwardResult["message"] = f"Forward RPC error: {e.details()}"
            except Exception as e:
                forwardResult["success"] = False
                forwardResult["message"] = f"Forward error: {e}"
        try:
            for request in request_iterator:
                if chunkHandle is None:
                    chunkHandle = request.chunk_handle
                    forwardAddresses = list(request.forward_addresses)
                    diskThread = threading.Thread(target=diskWorker, args = (chunkHandle, ), daemon=True)
                    diskThread.start()
                    if forwardAddresses:
                        forwardThread = threading.Thread(target=forwardWorker, args=(forwardAddresses[0], chunkHandle, forwardAddresses[1:]), daemon=True)
                        forwardThread.start()
                diskQueue.put(request.data)
                if forwardThread is not None:
                    forwardQueue.put(request.data)
            
            if chunkHandle is None:
                return WriteChunkResponse(success = False, message = "Empty write stream")
            
            diskQueue.put(None)
            if forwardThread is not None: forwardQueue.put(None)
            
            diskThread.join()
            if forwardThread is not None: forwardThread.join()

            if not diskResult["success"]:
                return WriteChunkResponse(success = False, message = f"Disk write failed: {diskResult['message']}")

            if not forwardResult["success"]:
                return WriteChunkResponse(success = False, message = f"Replica forwarding failed: {forwardResult['message']}")
            
            self.indexQueue.put(chunkHandle)
            return WriteChunkResponse(success = True, message = "Chunk Written")
        
        except Exception as ex:
            print(f"WriteChunk Error: {ex}")
            try: diskQueue.put_nowait(None)
            except Exception: pass
            try: forwardQueue.put_nowait(None)
            except Exception: pass
            return WriteChunkResponse(success=False, message=f"WriteChunk failed: {ex}")
    
    def ReadChunk(self, request: ReadChunkRequest, 
                  context: grpc.ServicerContext) -> Iterator[ReadChunkResponse]:
        chunkHandle = request.chunk_handle
        with self.lock.readLock():
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
        with self.lock.readLock():
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
        chunkHandle = request.chunk_handle
        path = self.store.getChunkPath(chunkHandle)
        try:
            channel = grpc.insecure_channel(request.source_address, options=GRPC_OPTIONS)
            stub = ChunkServerServiceStub(channel)
            with open(path, "wb") as f:
                for response in stub.ReadChunk(
                    ReadChunkRequest(chunk_handle = chunkHandle)
                ):
                    f.write(response.data)
            
            self.indexQueue.put(chunkHandle)
            return ReplicateChunkResponse(success = True, message = "Chunk Replicated")
        
        except grpc.RpcError as rpcError:
            self.store.deleteChunk(chunkHandle)
            return ReplicateChunkResponse(success = False, message = f"Replicate RPC Error: {rpcError.details()}")
        except Exception as ex:
            self.store.deleteChunk(chunkHandle)
            return ReplicateChunkResponse(success = False, message = f"Replication error {ex}")
        
    def ScanChunk(self,
                  request: ScanChunkRequest,
                  context: grpc.ServicerContext) -> ScanChunkResponse:
        chunkHandle = request.chunk_handle
        query = request.query
        if not query:
            return ScanChunkResponse()

        with self.lock.readLock():
            if not self.store.chunkExists(chunkHandle = chunkHandle):
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Chunk does not exist")
                return ScanChunkResponse()
            data = self.store.readChunk(chunkHandle = chunkHandle)
            candidates = self.ngramIndex.candidatePositions(chunkHandle, query)

        text = data.decode("utf-8", errors = "replace")

        if candidates is None:
            return self._scanLinear(text, query)
        if not candidates:
            return ScanChunkResponse()
        return self._scanByCandidates(text, query, candidates)

    def _scanLinear(self, text: str, query: str) -> ScanChunkResponse:
        pattern = re.compile(re.escape(query), re.IGNORECASE)
        matches = []
        for lineNumber, line in enumerate(text.splitlines()):
            if pattern.search(line):
                matches.append(ScanMatch(line_number = lineNumber, line_text = line))
        return ScanChunkResponse(matches = matches)

    def _scanByCandidates(self, text: str, query: str, candidates: list) -> ScanChunkResponse:
        # str.lower preserves length for all chars we care about; positions in
        # `candidates` index into text directly.
        lower = text.lower()
        qlow = query.lower()
        qlen = len(qlow)

        linesWithEnds = text.splitlines(keepends = True)
        lineStarts = []
        offset = 0
        for line in linesWithEnds:
            lineStarts.append(offset)
            offset += len(line)
        lines = [line.rstrip("\r\n") for line in linesWithEnds]

        matches = []
        seenLines = set()
        for p in candidates:
            if p + qlen > len(lower) or lower[p:p + qlen] != qlow:
                continue
            lineNumber = bisect.bisect_right(lineStarts, p) - 1
            if lineNumber in seenLines:
                continue
            seenLines.add(lineNumber)
            matches.append(ScanMatch(
                line_number = lineNumber,
                line_text = lines[lineNumber],
            ))
        matches.sort(key = lambda m: m.line_number)
        return ScanChunkResponse(matches = matches)

    def DeleteChunk(self,
                    request: DeleteChunkRequest,
                    context: grpc.ServicerContext) -> DeleteChunkResponse:
        chunkHandle = request.chunk_handle
        with self.lock.writeLock():
            if not self.store.chunkExists(chunkHandle = chunkHandle):
                return DeleteChunkResponse(success = True, message = "Chunk not present")
            self.store.deleteChunk(chunkHandle = chunkHandle)
            self.index.removeChunk(chunkHandle)
            self.ngramIndex.removeChunk(chunkHandle)
        return DeleteChunkResponse(success = True, message = "Chunk deleted")


def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type = int, required = True)
    parser.add_argument("--port", type = int, required = True)
    args = parser.parse_args()

    dataDir = f"/app/data"
    chunkServer = ChunkServer(serverId = args.id, port = args.port, dataDir = dataDir)

    server = grpc.server(ThreadPoolExecutor(max_workers = 10), options=GRPC_OPTIONS)
    add_ChunkServerServiceServicer_to_server(chunkServer, server)
    add_HeartbeatServiceServicer_to_server(chunkServer, server)

    server.add_insecure_port(f"0.0.0.0:{args.port}")
    server.start()
    print(f"Chunk server {args.id} started on port {args.port}")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()