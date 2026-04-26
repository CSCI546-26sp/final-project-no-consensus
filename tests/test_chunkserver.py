import os
import shutil
import tempfile
import time
import grpc
from concurrent.futures import ThreadPoolExecutor

from proto import chunkserver_pb2, chunkserver_pb2_grpc, heartbeat_pb2, heartbeat_pb2_grpc
from chunkserver.server import ChunkServer

TEST_PORT = 6099
TEST_DIR = os.path.join(tempfile.gettempdir(), "dfs_test_chunkserver")

def startTestServer():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)

    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    chunkServer = ChunkServer(serverId=99, port=TEST_PORT, dataDir=TEST_DIR)
    chunkserver_pb2_grpc.add_ChunkServerServiceServicer_to_server(chunkServer, server)
    heartbeat_pb2_grpc.add_HeartbeatServiceServicer_to_server(chunkServer, server)
    server.add_insecure_port(f"0.0.0.0:{TEST_PORT}")
    server.start()
    return server, chunkServer


def waitForIndex(chunkServer, chunkHandle, timeout=2.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if chunkHandle in chunkServer.ngramIndex.index:
            return
        time.sleep(0.05)
    raise TimeoutError(f"chunk {chunkHandle} not indexed within {timeout}s")

def writeChunkHelper(stub, chunkHandle, data):
    STREAM_SIZE = 512 * 1024

    def requestIterator():
        for i in range(0, len(data), STREAM_SIZE):
            yield chunkserver_pb2.WriteChunkRequest(
                chunk_handle=chunkHandle,
                data=data[i:i + STREAM_SIZE],
                forward_addresses=[]
            )

    return stub.WriteChunk(requestIterator())

def readChunkHelper(stub, chunkHandle):
    data = bytearray()
    for response in stub.ReadChunk(chunkserver_pb2.ReadChunkRequest(chunk_handle=chunkHandle)):
        data.extend(response.data)
    return bytes(data)

def testWriteAndRead(chunkStub):
    data = b"hello world this is chunk server test data"
    response = writeChunkHelper(chunkStub, "chunk-001", data)
    assert response.success == True, "WriteChunk should succeed"

    readData = readChunkHelper(chunkStub, "chunk-001")
    assert readData == data, f"Data mismatch: expected {data}, got {readData}"
    print("PASS: WriteChunk and ReadChunk")

def testReadNotFound(chunkStub):
    try:
        readChunkHelper(chunkStub, "nonexistent")
        assert False, "Should have raised an error"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND, \
            f"Expected NOT_FOUND, got {e.code()}"
    print("PASS: ReadChunk not found")

def testLargeChunk(chunkStub):
    data = b"x" * (4 * 1024 * 1024)
    response = writeChunkHelper(chunkStub, "chunk-large", data)
    assert response.success == True, "WriteChunk should succeed for large chunk"

    readData = readChunkHelper(chunkStub, "chunk-large")
    assert len(readData) == len(data), f"Size mismatch: expected {len(data)}, got {len(readData)}"
    assert readData == data, "Large chunk data mismatch"
    print("PASS: 4 MB chunk write and read")

def testSearchChunks(searchStub, chunkStub):
    writeChunkHelper(chunkStub, "chunk-search1", b"quantum physics experiment")
    writeChunkHelper(chunkStub, "chunk-search2", b"classical physics theory")
    writeChunkHelper(chunkStub, "chunk-search3", b"biology evolution darwin")

    response = searchStub.SearchChunks(heartbeat_pb2.SearchChunksRequest(query="physics"))
    assert len(response.matches) == 2, f"Expected 2 matches, got {len(response.matches)}"

    chunkHandles = [m.chunk_handle for m in response.matches]
    assert "chunk-search1" in chunkHandles, "chunk-search1 should match"
    assert "chunk-search2" in chunkHandles, "chunk-search2 should match"
    assert "chunk-search3" not in chunkHandles, "chunk-search3 should not match"
    print("PASS: SearchChunks")

def testSearchNoResults(searchStub):
    response = searchStub.SearchChunks(heartbeat_pb2.SearchChunksRequest(query="nonexistent"))
    assert len(response.matches) == 0, f"Expected 0 matches, got {len(response.matches)}"
    print("PASS: SearchChunks no results")


def testNgramIndexBuiltOnWrite(chunkStub, chunkServer):
    handle = "chunk-ngram-1"
    writeChunkHelper(chunkStub, handle, b"the quick brown fox jumps over the lazy dog")
    waitForIndex(chunkServer, handle)

    candidates = chunkServer.ngramIndex.candidatePositions(handle, "brown")
    assert candidates == [10], f"expected [10], got {candidates}"

    assert chunkServer.ngramIndex.candidatePositions(handle, "zebra") == []
    print("PASS: ngram index populated on WriteChunk")


def testScanChunkUsesTrigramIndex(chunkStub, chunkServer):
    handle = "chunk-scan-1"
    text = b"line one has cat\nline two has dog\nline three has bird and cat\nfourth line"
    writeChunkHelper(chunkStub, handle, text)
    waitForIndex(chunkServer, handle)

    response = chunkServer.ScanChunk(
        heartbeat_pb2.ScanChunkRequest(chunk_handle=handle, query="cat"), context=None
    )
    lineNumbers = sorted(m.line_number for m in response.matches)
    assert lineNumbers == [0, 2], f"expected lines [0, 2], got {lineNumbers}"
    # one match per line, even when the term appears elsewhere on the same line
    assert len(response.matches) == 2

    # case-insensitive
    response = chunkServer.ScanChunk(
        heartbeat_pb2.ScanChunkRequest(chunk_handle=handle, query="CAT"), context=None
    )
    assert sorted(m.line_number for m in response.matches) == [0, 2]

    # query that doesn't exist
    response = chunkServer.ScanChunk(
        heartbeat_pb2.ScanChunkRequest(chunk_handle=handle, query="zebra"), context=None
    )
    assert response.matches == []
    print("PASS: ScanChunk uses trigram index for >=3 char queries")


def testScanChunkShortQueryFallback(chunkStub, chunkServer):
    handle = "chunk-scan-2"
    writeChunkHelper(chunkStub, handle, b"the quick brown fox\nlazy dog at rest")
    waitForIndex(chunkServer, handle)

    # 2-char query bypasses trigram index, falls back to linear regex scan
    response = chunkServer.ScanChunk(
        heartbeat_pb2.ScanChunkRequest(chunk_handle=handle, query="at"), context=None
    )
    lineNumbers = sorted(m.line_number for m in response.matches)
    # "at" appears in "lazy dog at rest" (line 1)
    assert 1 in lineNumbers, f"expected line 1 in {lineNumbers}"
    print("PASS: ScanChunk falls back to linear scan for short queries")


def testNgramIndexEvictedOnDelete(chunkStub, chunkServer):
    handle = "chunk-ngram-2"
    writeChunkHelper(chunkStub, handle, b"alpha beta gamma delta")
    waitForIndex(chunkServer, handle)
    assert handle in chunkServer.ngramIndex.index

    chunkServer.DeleteChunk(
        heartbeat_pb2.DeleteChunkRequest(chunk_handle=handle), context=None
    )
    assert handle not in chunkServer.ngramIndex.index
    assert handle not in chunkServer.index.chunkTerms
    print("PASS: ngram index evicted on DeleteChunk")


def main():
    server, chunkServer = startTestServer()
    time.sleep(0.5)

    channel = grpc.insecure_channel(f"localhost:{TEST_PORT}")
    chunkStub = chunkserver_pb2_grpc.ChunkServerServiceStub(channel)
    searchStub = heartbeat_pb2_grpc.HeartbeatServiceStub(channel)

    print("\n--- Chunk Server Tests ---\n")

    try:
        testWriteAndRead(chunkStub)
        testReadNotFound(chunkStub)
        testLargeChunk(chunkStub)
        testSearchChunks(searchStub, chunkStub)
        testSearchNoResults(searchStub)
        testNgramIndexBuiltOnWrite(chunkStub, chunkServer)
        testScanChunkUsesTrigramIndex(chunkStub, chunkServer)
        testScanChunkShortQueryFallback(chunkStub, chunkServer)
        testNgramIndexEvictedOnDelete(chunkStub, chunkServer)
        print("\n--- All Chunk Server tests passed ---\n")
    except AssertionError as e:
        print(f"\nTEST FAILED: {e}\n")
    except Exception as e:
        print(f"\nERROR: {e}\n")
    finally:
        channel.close()
        server.stop(grace=0)
        if os.path.exists(TEST_DIR):
            shutil.rmtree(TEST_DIR)

if __name__ == "__main__":
    main()
