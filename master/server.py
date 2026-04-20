from collections import defaultdict
import threading
import time
import uuid
import math
import grpc
from concurrent.futures import ThreadPoolExecutor

from common.config import CHUNK_SERVER_PORTS, CHUNK_SIZE, MASTER_PORT, REPLICATION_FACTOR, HEARTBEAT_INTERVAL, HEARTBEAT_MISS_LIMIT
from common.config import GC_INTERVAL, GC_THRESHOLD
from proto.master_pb2_grpc import MasterServiceServicer
from proto.heartbeat_pb2 import ChunkMatch
from proto.heartbeat_pb2_grpc import HeartbeatServiceServicer, HeartbeatServiceStub
from proto import master_pb2, heartbeat_pb2, master_pb2_grpc, heartbeat_pb2_grpc

from proto.master_pb2 import FileSearchRequest, FileSearchResponse, FileSearchMatch

class FileMetaData:
    filename: str
    filesize: int
    num_chunks: int
    def __init__(self, filename, filesize = 0, num_chunks = 0):
        self.filename = filename
        self.filesize = filesize
        self.num_chunks = num_chunks

class ServerInfo:
    serverId: int
    availableDisk: int
    usedBytes: int
    lastHeartbeat: float
    alive: bool
    chunkHandles: set[str]
    def __init__(self, serverId, availableDisk, usedBytes = 0):
        self.serverId = serverId
        self.availableDisk = availableDisk
        self.usedBytes = usedBytes
        self.lastHeartbeat = 0
        self.alive = True
        self.chunkHandles = set()

class MasterServer(MasterServiceServicer, HeartbeatServiceServicer):
    def __init__(self):
        self.namespace: dict[str, FileMetaData] = {}
        self.fileToChunks: dict[str, list[str]] = defaultdict(list)
        self.chunkToServers: dict[str, set[int]] = {}
        self.chunkPrimary: dict[str, int] = {}
        self.serverInfo: dict[int, ServerInfo] = {}
        self.lock = threading.Lock()
        self.deletedFiles: dict[str, float] = {}
        threading.Thread(target=self._healthCheck, daemon=True).start()
        threading.Thread(target=self._garbageCollect, daemon=True).start()
        return
    
    def selectServers(self, replicationFactor:int, exceptions: set[int] = None) -> list[ServerInfo]:
        serverList = sorted([server for server in self.serverInfo.values()
                             if server.alive and (not exceptions or server.serverId not in exceptions)],
                            key = lambda server: (server.usedBytes, server.serverId))
        if len(serverList) < replicationFactor:
            print(f"Warning: Only {len(serverList)} servers alive; requested {replicationFactor}")
        return serverList[0:replicationFactor]
    
    def UploadFile(self, 
                   request : master_pb2.UploadFileRequest, context) -> master_pb2.UploadFileResponse:
        with self.lock:
            if request.filename in self.namespace:
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details("File already exists")
                return master_pb2.UploadFileResponse(success=False, 
                                                     message="File already exists")
            
            numChunks = math.ceil(request.file_size / CHUNK_SIZE)
            assignments = []
            for i in range(numChunks):
                chunkHandle = str(uuid.uuid4())
                chunkBytes = min(CHUNK_SIZE, request.file_size - i * CHUNK_SIZE)
                servers = self.selectServers(REPLICATION_FACTOR)

                serverAddresses = [f"dfs-chunk{s.serverId}:{CHUNK_SERVER_PORTS[s.serverId]}"
                                   for s in servers]
                self.fileToChunks[request.filename].append(chunkHandle)
                self.chunkToServers[chunkHandle] = set(server.serverId for server in servers)
                if servers:
                    self.chunkPrimary[chunkHandle] = servers[0].serverId
                    for s in servers:
                        self.serverInfo[s.serverId].usedBytes += chunkBytes

                assignments.append(master_pb2.ChunkAssignment(chunk_handle = chunkHandle,
                                                              chunk_index = i,
                                                              server_addresses = serverAddresses))
                
            self.namespace[request.filename] = FileMetaData(request.filename, 
                                                            request.file_size, 
                                                            numChunks)
            
            return master_pb2.UploadFileResponse(success = True, assignments = assignments)
        
    def DownloadFile(self, 
                     request : master_pb2.DownloadFileRequest, context) -> master_pb2.DownloadFileResponse:
        with self.lock:
            if request.filename.startswith("._deleted_"):
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Deleted file needs to be recovered before downloading")
                return master_pb2.DownloadFileResponse(success = False, 
                                                       message = "File not found in current namespace, " \
                                                       "needs to be recovered first")
            
            if request.filename not in self.namespace:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("File not found in current namespace")
                return master_pb2.DownloadFileResponse(success = False, 
                                                       message = "File not found in current namespace")
            
            chunkHandles = self.fileToChunks[request.filename]
            locations = []

            for idx, chunkHandle in enumerate(chunkHandles):
                serverIds = self.chunkToServers[chunkHandle]
                aliveAddresses = [
                    f"dfs-chunk{s}:{CHUNK_SERVER_PORTS[s]}" for s in serverIds 
                    if self.serverInfo[s].alive == True
                ]
                locations.append(master_pb2.ChunkLocation(chunk_handle = chunkHandle, 
                                                          chunk_index = idx,
                                                          server_addresses = aliveAddresses))
            return master_pb2.DownloadFileResponse(success = True, locations = locations)
        
    def DeleteFile(self, 
                   request: master_pb2.DeleteFileRequest, context) -> master_pb2.DeleteFileResponse:
        with self.lock:
            if request.filename.startswith("._deleted_"):
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("File is already deleted")
                return master_pb2.DeleteFileResponse(success = False, 
                                                       message = "File is already deleted")
            
            if request.filename not in self.namespace:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("File not found in current namespace")
                return master_pb2.DeleteFileResponse(success = False, 
                                                     message = "File not found in current namespace")
            hiddenName = f"._deleted_{time.time()}_{request.filename}"
            self.namespace[hiddenName] = self.namespace.pop(request.filename)
            self.fileToChunks[hiddenName] = self.fileToChunks.pop(request.filename)

            self.deletedFiles[hiddenName] = time.time()
            return master_pb2.DeleteFileResponse(success = True, message = "File marked for deletion")
    
    def ListFiles(self, request: master_pb2.ListFilesRequest, context) -> master_pb2.ListFilesResponse:
        with self.lock:
            files = []
            for filename, meta in self.namespace.items():
                if filename.startswith("._deleted_"):
                    continue
                chunkInfos = []
                for idx, chunkHandle in enumerate(self.fileToChunks.get(filename, [])):
                    replicas = self.chunkToServers.get(chunkHandle, set())
                    aliveReplicas = {s for s in replicas if self.serverInfo.get(s) and self.serverInfo[s].alive}
                    primary = self.chunkPrimary.get(chunkHandle)
                    if primary not in aliveReplicas:
                        primary = next(iter(sorted(aliveReplicas)), None)
                    ordered = ([primary] if primary is not None else []) + sorted(r for r in replicas if r != primary)
                    chunkInfos.append(master_pb2.ChunkInfo(
                        chunk_handle = chunkHandle,
                        chunk_index = idx,
                        server_ids = ordered,
                    ))
                files.append(master_pb2.FileInfo(
                    filename = filename,
                    file_size = meta.filesize,
                    num_chunks = meta.num_chunks,
                    chunks = chunkInfos,
                ))
            return master_pb2.ListFilesResponse(files = files)
    
    def SearchFiles(self, 
                    request: master_pb2.SearchFilesRequest, context) -> master_pb2.SearchFilesResponse:
        with self.lock:
            addressesOfServersThatAreAliveAndDoingWell = [
                        f"dfs-chunk{s.serverId}:{CHUNK_SERVER_PORTS[s.serverId]}" for s in self.serverInfo.values() 
                        if s.alive == True
                    ]
        
        allMatches: list[ChunkMatch] = []
        with ThreadPoolExecutor() as commanderExecutor:
            futures = [
                commanderExecutor.submit(self._fanOut, serverAddress, request)
                for serverAddress in addressesOfServersThatAreAliveAndDoingWell
            ]

            for result in futures:
                allMatches.extend(result.result())

        with self.lock:
            chunkToFile = {}
            for filename, chunkHandles in self.fileToChunks.items():
                if filename.startswith("._deleted_"):
                    continue
                for chunkHandle in chunkHandles:
                    chunkToFile[chunkHandle] = filename

        fileScores = {}
        for match in allMatches:
            filename = chunkToFile.get(match.chunk_handle)
            if filename is None:
                continue

            if filename not in fileScores:
                fileScores[filename] = [0, match.line_number, match.snippet, match.score, match.chunk_handle]
            fileScores[filename][0] += match.score

            if match.score > fileScores[filename][3]:
                fileScores[filename][1] = match.line_number
                fileScores[filename][2] = match.snippet
                fileScores[filename][3] = match.score
                fileScores[filename][4] = match.chunk_handle

        sortedResults = sorted(fileScores.items(), key = lambda x: x[1][0], reverse = True)
        return master_pb2.SearchFilesResponse(results = [
            master_pb2.SearchResult(filename=filename,
                                    chunk_handle=chunkHandle,
                                    score=score,
                                    line_number=lineNumber,
                                    snippet=snippet)
                for (filename, [score, lineNumber, snippet, _, chunkHandle]) in sortedResults
            ])

    def FileSearch(self, request: FileSearchRequest, context: grpc.ServicerContext) -> FileSearchResponse:
        if not request.query:
            return FileSearchResponse(success = False, message = "Empty query")
        
        with self.lock:
            if request.filename.startswith("._deleted_") or request.filename not in self.namespace:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("File not found in current namespace")
                return FileSearchResponse(success = False, message = "File not found in current namespace")
            
            chunkHandles = list(self.fileToChunks[request.filename])
            tasks = []
            for idx, chunkHandle in enumerate(chunkHandles):
                serverIds = self.chunkToServers.get(chunkHandle, set())
                aliveIds = [s for s in serverIds if self.serverInfo[s].alive]
                if not aliveIds:
                    continue
                serverId = next(iter(aliveIds))
                address = f"dfs-chunk{serverId}:{CHUNK_SERVER_PORTS[serverId]}"
                tasks.append((idx, chunkHandle, address))
            
        results = []
        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self._scanFanOut, address, chunkHandle, request.query): idx for (idx, chunkHandle, address) in tasks
            }
            for future in futures:
                idx = futures[future]
                for scanMatch in future.result():
                    results.append((idx, scanMatch))
        results.sort(key = lambda pair: (pair[0], pair[1].line_number))

        matches = [
            FileSearchMatch(
                chunk_index = idx,
                line_number = scanMatch.line_number,
                line_text = scanMatch.line_text
            )
            for (idx, scanMatch) in results
        ]
        return FileSearchResponse(success = True, matches = matches)
    
    def _scanFanOut(self, serverAddress: str, chunkHandle: str, query: str):
        try:
            channel = grpc.insecure_channel(serverAddress)
            stub = HeartbeatServiceStub(channel)
            response = stub.ScanChunk(heartbeat_pb2.ScanChunkRequest(
                chunk_handle = chunkHandle,
                query = query
            ))
            return response.matches
        except grpc.RpcError:
            return []

    def _fanOut(self, serverAddress: str, request: master_pb2.SearchFilesRequest):
        try:
            channel = grpc.insecure_channel(serverAddress)
            stub = HeartbeatServiceStub(channel)
            response = stub.SearchChunks(heartbeat_pb2.SearchChunksRequest(query=request.query))
            return response.matches
        except grpc.RpcError as e:
            return []
    
    def Heartbeat(self, 
                  request: heartbeat_pb2.HeartbeatRequest, context) -> heartbeat_pb2.HeartbeatResponse:
        with self.lock:
            serverId = request.server_id
            if serverId not in self.serverInfo:
                self.serverInfo[serverId] = ServerInfo(serverId=serverId, availableDisk=request.available_disk)
            
            server = self.serverInfo[serverId]
            server.availableDisk = request.available_disk
            server.usedBytes = request.used_bytes
            server.lastHeartbeat = time.time()
            server.alive = True
            server.chunkHandles = set(request.chunk_handles)

            for chunkHandle in request.chunk_handles:
                if chunkHandle in self.chunkToServers:
                    self.chunkToServers[chunkHandle].add(serverId)

            return heartbeat_pb2.HeartbeatResponse(success = True)
    
    def _healthCheck(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            replicationTasks = []

            with self.lock:
                now = time.time()
                timeout = HEARTBEAT_INTERVAL * HEARTBEAT_MISS_LIMIT
                for (serverId, server) in self.serverInfo.items():
                    if server.alive and (now - server.lastHeartbeat) > timeout:
                        server.alive = False
                        print(f"Server {serverId} marked as dead")
                        
                        for chunkHandle in server.chunkHandles:
                            if chunkHandle in self.chunkToServers:
                                self.chunkToServers[chunkHandle].discard(serverId)
                                aliveReplicas = len(self.chunkToServers[chunkHandle])
                                
                                if 0 < aliveReplicas < REPLICATION_FACTOR:    
                                    sourceId = next(iter(self.chunkToServers[chunkHandle]))
                                    candidates = self.selectServers(
                                        1, 
                                        self.chunkToServers[chunkHandle])
                                    
                                    if candidates:
                                        targetId = candidates[0].serverId
                                        replicationTasks.append((chunkHandle, sourceId, targetId))
            
            for (chunkHandle, sourceId, targetId) in replicationTasks:
                sourceAddress = f"dfs-chunk{sourceId}:{CHUNK_SERVER_PORTS[sourceId]}"
                targetAddress = f"dfs-chunk{targetId}:{CHUNK_SERVER_PORTS[targetId]}"
                try:
                    channel = grpc.insecure_channel(targetAddress)
                    stub = HeartbeatServiceStub(channel)
                    stub.ReplicateChunk(heartbeat_pb2.ReplicateChunkRequest(
                        chunk_handle = chunkHandle, 
                        source_address = sourceAddress))
                    with self.lock:
                        self.chunkToServers[chunkHandle].add(targetId)
                    print(f"Chunk {chunkHandle} replicated to server {targetId}")
                except grpc.RpcError as error: 
                    print(f"Error while re-replicating {error.details()}")


    def _garbageCollect(self):
        while True:
            time.sleep(GC_INTERVAL)
            with self.lock:
                now = time.time()
                toRemove = []

                for hiddenName, deletionTime in self.deletedFiles.items():
                    if (now - deletionTime) > GC_THRESHOLD:
                        toRemove.append(hiddenName)
                
                for hiddenName in toRemove:
                    chunkHandles = self.fileToChunks.pop(hiddenName, [])
                    for chunkHandle in chunkHandles:
                        self.chunkToServers.pop(chunkHandle, None)
                        self.chunkPrimary.pop(chunkHandle, None)

                    self.namespace.pop(hiddenName, None)
                    del self.deletedFiles[hiddenName]

                    print(f"Garbage collected: {hiddenName}")

def serve():
    server = grpc.server(ThreadPoolExecutor(max_workers= 10))
    master = MasterServer()
    master_pb2_grpc.add_MasterServiceServicer_to_server(master, server)
    heartbeat_pb2_grpc.add_HeartbeatServiceServicer_to_server(master, server)

    server.add_insecure_port(f"0.0.0.0:{MASTER_PORT}")
    server.start()
    print(f"Master server started on port {MASTER_PORT}")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()