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
    return server

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

def main():
    server = startTestServer()
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
