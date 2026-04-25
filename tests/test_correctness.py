import common.config
import grpc
import builtins

TEST_MASTER_PORT = 5051
TEST_CHUNK_PORTS = {1: 7001, 2: 7002, 3: 7003}

# Mutate the existing dict (shared reference propagates everywhere)
common.config.CHUNK_SERVER_PORTS[1] = TEST_CHUNK_PORTS[1]
common.config.CHUNK_SERVER_PORTS[2] = TEST_CHUNK_PORTS[2]
common.config.CHUNK_SERVER_PORTS[3] = TEST_CHUNK_PORTS[3]

# Reassign scalars before any module copies them via 'from ... import'
common.config.MASTER_HOST = "localhost"
common.config.MASTER_PORT = TEST_MASTER_PORT
common.config.HEARTBEAT_INTERVAL = 2       # faster heartbeats for tests
common.config.GC_INTERVAL = 9999           # effectively disable GC
common.config.GC_THRESHOLD = 9999


_original_print = builtins.print
def _patched_print(*args, **kwargs):
    if args and isinstance(args[0], str) and "Heartbeat" in args[0]:
        return
    _original_print(*args, **kwargs)
builtins.print = _patched_print


_original_insecure_channel = grpc.insecure_channel
def _patched_insecure_channel(target, options=None, compression=None):
    if type(target) is str and target.startswith("dfs-"):
        port = target.split(":")[1]
        target = f"localhost:{port}"
    return _original_insecure_channel(target, options, compression)
grpc.insecure_channel = _patched_insecure_channel

#=========================

import hashlib
import math
import os
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

from common.config import CHUNK_SIZE, REPLICATION_FACTOR
from master.server import MasterServer
from chunkserver.server import ChunkServer
from proto import (
    master_pb2, master_pb2_grpc,
    heartbeat_pb2, heartbeat_pb2_grpc,
    chunkserver_pb2, chunkserver_pb2_grpc,
)

STREAM_SIZE = 512 * 1024  # 512 KB streaming pieces (same as client/app.py)

def startTestMaster():
    """Start an in-process MasterServer on TEST_MASTER_PORT."""
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    master = MasterServer()
    master_pb2_grpc.add_MasterServiceServicer_to_server(master, server)
    heartbeat_pb2_grpc.add_HeartbeatServiceServicer_to_server(master, server)
    server.add_insecure_port(f"0.0.0.0:{TEST_MASTER_PORT}")
    server.start()
    return server


def startTestChunkServer(serverId, port, dataDir):
    """Start an in-process ChunkServer on the given port."""
    chunkServer = ChunkServer(serverId=serverId, port=port, dataDir=dataDir)
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    chunkserver_pb2_grpc.add_ChunkServerServiceServicer_to_server(chunkServer, server)
    heartbeat_pb2_grpc.add_HeartbeatServiceServicer_to_server(chunkServer, server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    return server


def registerChunkServers(heartbeatStub):
    """Register 3 chunk servers with the master via Heartbeat RPC."""
    for serverId in range(1, 4):
        response = heartbeatStub.Heartbeat(heartbeat_pb2.HeartbeatRequest(
            server_id=serverId,
            available_disk=50 * 1024 * 1024 * 1024,  # 50 GB
            chunk_handles=[],
        ))
        assert response.success, f"Failed to register chunk server {serverId}"
    print("SETUP: Registered 3 chunk servers")


def translateAddress(dockerAddress: str) -> str:
    """Convert 'dfs-chunkN:port' → 'localhost:port'."""
    port = dockerAddress.split(":")[1]
    return f"localhost:{port}"

def uploadFile(masterStub, filename, data):
    """
    Full upload flow: UploadFile → WriteChunk to primary with chain
    replication forwarding.  Returns the UploadFileResponse.
    """
    size = len(data)
    response = masterStub.UploadFile(
        master_pb2.UploadFileRequest(filename=filename, file_size=size)
    )
    assert response.success, f"UploadFile failed: {response.message}"

    for assignment in response.assignments:
        chunkIndex = assignment.chunk_index
        chunkData = data[chunkIndex * CHUNK_SIZE : (chunkIndex + 1) * CHUNK_SIZE]

        addresses = list(assignment.server_addresses)
        primaryAddress = translateAddress(addresses[0])
        forwardAddresses = addresses[1:]  # keep docker addresses for chain repl

        channel = grpc.insecure_channel(primaryAddress)
        stub = chunkserver_pb2_grpc.ChunkServerServiceStub(channel)

        def makeIterator(chunk, chunkHandle, forwards):
            for i in range(0, len(chunk), STREAM_SIZE):
                yield chunkserver_pb2.WriteChunkRequest(
                    chunk_handle=chunkHandle,
                    data=chunk[i : i + STREAM_SIZE],
                    forward_addresses=forwards,
                )

        writeResp = stub.WriteChunk(
            makeIterator(chunkData, assignment.chunk_handle, forwardAddresses)
        )
        assert writeResp.success, \
            f"WriteChunk failed for chunk {chunkIndex}: {writeResp.message}"

    return response


def downloadFile(masterStub, filename):
    """
    Full download flow: DownloadFile → ReadChunk from each location →
    reassemble.  Returns the raw bytes.
    """
    response = masterStub.DownloadFile(
        master_pb2.DownloadFileRequest(filename=filename)
    )
    assert response.success, f"DownloadFile failed: {response.message}"

    locations = sorted(response.locations, key=lambda loc: loc.chunk_index)
    fileData = bytearray()

    for location in locations:
        address = translateAddress(location.server_addresses[0])
        channel = grpc.insecure_channel(address)
        stub = chunkserver_pb2_grpc.ChunkServerServiceStub(channel)
        for readResp in stub.ReadChunk(
            chunkserver_pb2.ReadChunkRequest(chunk_handle=location.chunk_handle)
        ):
            fileData.extend(readResp.data)

    return bytes(fileData)


# ════════════════════════════════════════════════════════════════
#  Test cases
# ════════════════════════════════════════════════════════════════

def testUploadDownloadChecksum(masterStub, sizeMB, label):
    """Upload duplicated big.txt, download, compare SHA-256."""
    with open("client/big.txt", "rb") as f:
        base_data = f.read()

    target_size = sizeMB * 1024 * 1024
    multiplier = math.ceil(target_size / max(1, len(base_data)))
    data = base_data * multiplier
    filename = f"checksum_test_{sizeMB}mb.bin"

    uploadFile(masterStub, filename, data)
    downloaded = downloadFile(masterStub, filename)

    origHash = hashlib.sha256(data).hexdigest()
    dlHash = hashlib.sha256(downloaded).hexdigest()
    assert origHash == dlHash, \
        f"SHA-256 mismatch for {label}: {origHash} != {dlHash}"

    # Cleanup
    masterStub.DeleteFile(master_pb2.DeleteFileRequest(filename=filename))
    print(f"PASS: Upload + download checksum ({label})")


def testSearchAccuracy(masterStub):
    """Upload known text files, search for terms, verify results."""
    files = {
        "search_doc_alpha.txt": b"distributed systems are fascinating and powerful\n"
                                b"they enable large scale computing\n",
        "search_doc_beta.txt":  b"machine learning algorithms transform data\n"
                                b"neural networks learn patterns\n",
    }

    for name, content in files.items():
        uploadFile(masterStub, name, content)

    # Wait briefly for index to be ready (write already indexes on the chunk server)
    time.sleep(0.5)

    # Search for "distributed" — should find alpha
    resp = masterStub.SearchFiles(master_pb2.SearchFilesRequest(query="distributed"))
    resultNames = [r.filename for r in resp.results]
    assert "search_doc_alpha.txt" in resultNames, \
        f"Expected 'search_doc_alpha.txt' in results, got {resultNames}"

    # Search for "algorithms" — should find beta
    resp = masterStub.SearchFiles(master_pb2.SearchFilesRequest(query="algorithms"))
    resultNames = [r.filename for r in resp.results]
    assert "search_doc_beta.txt" in resultNames, \
        f"Expected 'search_doc_beta.txt' in results, got {resultNames}"

    # Search for nonsense — should return empty
    resp = masterStub.SearchFiles(master_pb2.SearchFilesRequest(query="xyzzy_nonexistent"))
    assert len(resp.results) == 0, \
        f"Expected 0 results for nonsense query, got {len(resp.results)}"

    # Cleanup
    for name in files:
        masterStub.DeleteFile(master_pb2.DeleteFileRequest(filename=name))

    print("PASS: Search accuracy (hit, hit, miss)")


def testDeletion(masterStub):
    """Upload, delete, verify hidden from listing and download fails."""
    data = b"file to be deleted\n" * 100
    filename = "delete_test.txt"

    uploadFile(masterStub, filename, data)

    # Should appear in listing
    listing = masterStub.ListFiles(master_pb2.ListFilesRequest())
    names = [f.filename for f in listing.files]
    assert filename in names, f"File not in listing after upload: {names}"

    # Delete
    masterStub.DeleteFile(master_pb2.DeleteFileRequest(filename=filename))

    # Should be hidden from listing
    listing = masterStub.ListFiles(master_pb2.ListFilesRequest())
    names = [f.filename for f in listing.files]
    assert filename not in names, f"File still visible after delete: {names}"

    # Download should fail
    try:
        masterStub.DownloadFile(master_pb2.DownloadFileRequest(filename=filename))
        assert False, "DownloadFile should have raised NOT_FOUND after deletion"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND, \
            f"Expected NOT_FOUND, got {e.code()}"

    print("PASS: Deletion (hidden from listing, download rejected)")


def testReplicationVerification(masterStub):
    """Verify each chunk is stored on REPLICATION_FACTOR servers."""
    data = os.urandom(8 * 1024 * 1024)  # 8 MB = 2 chunks
    filename = "replication_test.bin"

    uploadFile(masterStub, filename, data)

    # Check via ListFiles
    listing = masterStub.ListFiles(master_pb2.ListFilesRequest())
    fileInfo = None
    for f in listing.files:
        if f.filename == filename:
            fileInfo = f
            break
    assert fileInfo is not None, "File not found in listing"

    for chunk in fileInfo.chunks:
        assert len(chunk.server_ids) >= REPLICATION_FACTOR, \
            f"Chunk {chunk.chunk_index} on {len(chunk.server_ids)} servers, " \
            f"expected >= {REPLICATION_FACTOR}"

    # Cleanup
    masterStub.DeleteFile(master_pb2.DeleteFileRequest(filename=filename))
    print(f"PASS: Replication verification ({REPLICATION_FACTOR} replicas per chunk)")


def testUploadDuplicateRejection(masterStub):
    """Upload a file, then try again — expect ALREADY_EXISTS."""
    data = b"duplicate test content\n"
    filename = "duplicate_test.txt"

    uploadFile(masterStub, filename, data)

    try:
        masterStub.UploadFile(
            master_pb2.UploadFileRequest(filename=filename, file_size=len(data))
        )
        assert False, "Should have raised ALREADY_EXISTS"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ALREADY_EXISTS, \
            f"Expected ALREADY_EXISTS, got {e.code()}"

    # Cleanup
    masterStub.DeleteFile(master_pb2.DeleteFileRequest(filename=filename))
    print("PASS: Upload duplicate rejection")


def testDownloadNonExistent(masterStub):
    """Download a file that was never uploaded — expect NOT_FOUND."""
    try:
        masterStub.DownloadFile(
            master_pb2.DownloadFileRequest(filename="does_not_exist_12345.txt")
        )
        assert False, "Should have raised NOT_FOUND"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND, \
            f"Expected NOT_FOUND, got {e.code()}"
    print("PASS: Download non-existent file rejected")


def testChainReplicationConsistency(masterStub):
    """
    Upload a file, then read each chunk from BOTH replica servers
    independently and verify they have identical data.
    """
    data = os.urandom(8 * 1024 * 1024)  # 8 MB = 2 chunks
    filename = "chain_repl_test.bin"

    uploadResponse = uploadFile(masterStub, filename, data)

    # Get chunk locations via ListFiles (gives us server_ids per chunk)
    listing = masterStub.ListFiles(master_pb2.ListFilesRequest())
    fileInfo = None
    for f in listing.files:
        if f.filename == filename:
            fileInfo = f
            break
    assert fileInfo is not None, "File not found in listing"

    for chunk in fileInfo.chunks:
        assert len(chunk.server_ids) >= 2, \
            f"Need >= 2 replicas for consistency check, got {len(chunk.server_ids)}"

        # Read from each replica independently
        replicaData = []
        for serverId in chunk.server_ids[:2]:
            port = TEST_CHUNK_PORTS[serverId]
            channel = grpc.insecure_channel(f"localhost:{port}")
            stub = chunkserver_pb2_grpc.ChunkServerServiceStub(channel)
            chunkBytes = bytearray()
            for resp in stub.ReadChunk(
                chunkserver_pb2.ReadChunkRequest(chunk_handle=chunk.chunk_handle)
            ):
                chunkBytes.extend(resp.data)
            replicaData.append(bytes(chunkBytes))

        assert replicaData[0] == replicaData[1], \
            f"Chunk {chunk.chunk_index}: replicas differ " \
            f"(server {chunk.server_ids[0]} vs {chunk.server_ids[1]})"

    # Cleanup
    masterStub.DeleteFile(master_pb2.DeleteFileRequest(filename=filename))
    print("PASS: Chain replication consistency (all replicas identical)")


def main():
    # Create temp data directories for chunk servers
    tmpDir = tempfile.mkdtemp(prefix="dfs_test_")
    chunkDirs = {}
    for sid in range(1, 4):
        d = os.path.join(tmpDir, f"chunk{sid}")
        os.makedirs(d)
        chunkDirs[sid] = d

    # Start servers
    masterServer = startTestMaster()
    chunkServers = []
    for sid in range(1, 4):
        cs = startTestChunkServer(sid, TEST_CHUNK_PORTS[sid], chunkDirs[sid])
        chunkServers.append(cs)
    time.sleep(3)  # let servers start

    # Create stubs
    masterChannel = grpc.insecure_channel(f"localhost:{TEST_MASTER_PORT}")
    masterStub = master_pb2_grpc.MasterServiceStub(masterChannel)
    heartbeatStub = heartbeat_pb2_grpc.HeartbeatServiceStub(masterChannel)

    print("\n--- End-to-End Correctness Tests ---\n")

    try:
        # Register chunk servers (same as test_master.py)
        registerChunkServers(heartbeatStub)

        # Run tests
        testUploadDownloadChecksum(masterStub, 1, "1 MB")
        testUploadDownloadChecksum(masterStub, 10, "10 MB")
        testUploadDownloadChecksum(masterStub, 100, "100 MB")
        # testSearchAccuracy(masterStub)
        # testDeletion(masterStub)
        # testReplicationVerification(masterStub)
        # testUploadDuplicateRejection(masterStub)
        # testDownloadNonExistent(masterStub)
        # testChainReplicationConsistency(masterStub)

        print("\n--- All tests passed ---\n")

    except AssertionError as e:
        print(f"\nTEST FAILED: {e}\n")
    except Exception as e:
        print(f"\nERROR: {e}\n")
        import traceback
        traceback.print_exc()
    finally:
        masterChannel.close()
        for cs in chunkServers:
            cs.stop(grace=0)
        masterServer.stop(grace=0)
        shutil.rmtree(tmpDir, ignore_errors=True)


if __name__ == "__main__":
    main()
