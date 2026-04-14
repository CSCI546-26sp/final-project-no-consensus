import grpc
import math
import hashlib
import time

from common.config import CHUNK_SIZE, CHUNK_SERVER_PORTS
from proto import master_pb2, master_pb2_grpc, chunkserver_pb2, chunkserver_pb2_grpc

MASTER_ADDRESS = "localhost:5050"

def translateAddress(dockerAddress: str) -> str:
    """Translate Docker-internal address to localhost for host-side testing.
    e.g. dfs-chunk1:6001 -> localhost:6001"""
    port = dockerAddress.split(":")[1]
    return f"localhost:{port}"

def uploadFile(masterStub, filename: str, fileData: bytes):
    """Upload a file: get assignments from master, stream chunks to chunk servers."""
    fileSize = len(fileData)
    response = masterStub.UploadFile(master_pb2.UploadFileRequest(
        filename=filename,
        file_size=fileSize
    ))
    assert response.success, f"UploadFile failed: {response.message}"

    expectedChunks = math.ceil(fileSize / CHUNK_SIZE)
    assert len(response.assignments) == expectedChunks, \
        f"Expected {expectedChunks} assignments, got {len(response.assignments)}"

    for assignment in response.assignments:
        chunkIndex = assignment.chunk_index
        chunkStart = chunkIndex * CHUNK_SIZE
        chunkEnd = min(chunkStart + CHUNK_SIZE, fileSize)
        chunkData = fileData[chunkStart:chunkEnd]

        # First address is primary, rest are forwarding targets
        addresses = list(assignment.server_addresses)
        primaryAddress = translateAddress(addresses[0])
        forwardAddresses = addresses[1:]  # keep Docker-internal for chain replication

        channel = grpc.insecure_channel(primaryAddress)
        stub = chunkserver_pb2_grpc.ChunkServerServiceStub(channel)

        STREAM_SIZE = 512 * 1024
        def makeIterator(data, chunkHandle, forwards):
            for i in range(0, len(data), STREAM_SIZE):
                yield chunkserver_pb2.WriteChunkRequest(
                    chunk_handle=chunkHandle,
                    data=data[i:i + STREAM_SIZE],
                    forward_addresses=forwards
                )

        writeResponse = stub.WriteChunk(makeIterator(chunkData, assignment.chunk_handle, forwardAddresses))
        assert writeResponse.success, f"WriteChunk failed for chunk {chunkIndex}: {writeResponse.message}"

    return response.assignments

def downloadFile(masterStub, filename: str) -> bytes:
    """Download a file: get locations from master, read chunks from chunk servers."""
    response = masterStub.DownloadFile(master_pb2.DownloadFileRequest(filename=filename))
    assert response.success, f"DownloadFile failed: {response.message}"

    # Sort by chunk_index to reassemble in order
    locations = sorted(response.locations, key=lambda loc: loc.chunk_index)
    fileData = bytearray()

    for location in locations:
        address = translateAddress(location.server_addresses[0])
        channel = grpc.insecure_channel(address)
        stub = chunkserver_pb2_grpc.ChunkServerServiceStub(channel)

        chunkData = bytearray()
        for readResponse in stub.ReadChunk(chunkserver_pb2.ReadChunkRequest(
                chunk_handle=location.chunk_handle)):
            chunkData.extend(readResponse.data)

        fileData.extend(chunkData)

    return bytes(fileData)

# --- Tests ---

def testUploadAndDownload(masterStub):
    data = b"Hello from the distributed file system! " * 100
    uploadFile(masterStub, "test_small.txt", data)

    downloaded = downloadFile(masterStub, "test_small.txt")
    assert downloaded == data, "Downloaded data does not match uploaded data"
    print("PASS: Upload and download small file - data integrity verified")

def testLargeFile(masterStub):
    # 10 MB file - spans 3 chunks (4MB + 4MB + 2MB)
    data = b"Line of text for large file testing.\n" * 300000
    data = data[:10 * 1024 * 1024]  # trim to exactly 10 MB

    uploadFile(masterStub, "test_large.txt", data)

    downloaded = downloadFile(masterStub, "test_large.txt")
    uploadHash = hashlib.sha256(data).hexdigest()
    downloadHash = hashlib.sha256(downloaded).hexdigest()
    assert uploadHash == downloadHash, \
        f"SHA256 mismatch: upload={uploadHash}, download={downloadHash}"
    print(f"PASS: Upload and download 10MB file - SHA256 verified ({uploadHash[:16]}...)")

def testListFiles(masterStub):
    response = masterStub.ListFiles(master_pb2.ListFilesRequest())
    filenames = [f.filename for f in response.files]
    assert "test_small.txt" in filenames, "test_small.txt should be listed"
    assert "test_large.txt" in filenames, "test_large.txt should be listed"
    print(f"PASS: ListFiles - {len(response.files)} files listed")

def testSearchFiles(masterStub):
    # Upload a file with known searchable content
    data = b"quantum physics experiment\nclassical mechanics theory\nbiology evolution darwin\n"
    uploadFile(masterStub, "test_search.txt", data)

    response = masterStub.SearchFiles(master_pb2.SearchFilesRequest(query="physics"))
    assert len(response.results) > 0, "Expected at least one search result"

    filenames = [r.filename for r in response.results]
    assert "test_search.txt" in filenames, "test_search.txt should match 'physics'"
    print(f"PASS: SearchFiles - found {len(response.results)} result(s) for 'physics'")

def testSearchNoResults(masterStub):
    response = masterStub.SearchFiles(master_pb2.SearchFilesRequest(query="xyznonexistent"))
    assert len(response.results) == 0, f"Expected 0 results, got {len(response.results)}"
    print("PASS: SearchFiles - no results for non-existent term")

def testDeleteFile(masterStub):
    response = masterStub.DeleteFile(master_pb2.DeleteFileRequest(filename="test_small.txt"))
    assert response.success, f"DeleteFile failed: {response.message}"

    # Verify it's gone from listing
    listResponse = masterStub.ListFiles(master_pb2.ListFilesRequest())
    filenames = [f.filename for f in listResponse.files]
    assert "test_small.txt" not in filenames, "test_small.txt should not be listed after delete"

    # Verify download fails
    try:
        masterStub.DownloadFile(master_pb2.DownloadFileRequest(filename="test_small.txt"))
        assert False, "Should have raised NOT_FOUND"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND

    print("PASS: DeleteFile - file removed from listing and download blocked")

def testUploadDuplicate(masterStub):
    try:
        masterStub.UploadFile(master_pb2.UploadFileRequest(
            filename="test_large.txt",
            file_size=1024
        ))
        assert False, "Should have raised ALREADY_EXISTS"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ALREADY_EXISTS
    print("PASS: Upload duplicate rejected")

def main():
    channel = grpc.insecure_channel(MASTER_ADDRESS)
    masterStub = master_pb2_grpc.MasterServiceStub(channel)

    print("\n--- End-to-End Tests ---\n")
    print("Waiting for cluster to stabilize...")
    time.sleep(2)

    try:
        testUploadAndDownload(masterStub)
        testLargeFile(masterStub)
        testListFiles(masterStub)
        testSearchFiles(masterStub)
        testSearchNoResults(masterStub)
        testDeleteFile(masterStub)
        testUploadDuplicate(masterStub)
        print("\n--- All E2E tests passed ---\n")
    except AssertionError as e:
        print(f"\nTEST FAILED: {e}\n")
    except grpc.RpcError as e:
        print(f"\nGRPC ERROR: {e.code()} - {e.details()}\n")
    except Exception as e:
        print(f"\nERROR: {e}\n")
        import traceback
        traceback.print_exc()
    finally:
        channel.close()

if __name__ == "__main__":
    main()
