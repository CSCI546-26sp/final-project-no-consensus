import grpc
import threading
import time
import math
from concurrent.futures import ThreadPoolExecutor

from common.config import CHUNK_SIZE, REPLICATION_FACTOR
from proto import master_pb2, master_pb2_grpc, heartbeat_pb2, heartbeat_pb2_grpc
from master.server import MasterServer

TEST_PORT = 5051

def startTestServer():
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    master = MasterServer()
    master_pb2_grpc.add_MasterServiceServicer_to_server(master, server)
    heartbeat_pb2_grpc.add_HeartbeatServiceServicer_to_server(master, server)
    server.add_insecure_port(f"0.0.0.0:{TEST_PORT}")
    server.start()
    return server

def registerChunkServers(heartbeatStub):
    for serverId in range(1, 4):
        response = heartbeatStub.Heartbeat(heartbeat_pb2.HeartbeatRequest(
            server_id=serverId,
            available_disk=50 * 1024 * 1024 * 1024,  # 50 GB
            chunk_handles=[]
        ))
        assert response.success == True, f"Failed to register chunk server {serverId}"
    print("PASS: Registered 3 chunk servers")

def testUploadFile(masterStub):
    fileSize = 10 * 1024 * 1024  # 10 MB
    expectedChunks = math.ceil(fileSize / CHUNK_SIZE)

    response = masterStub.UploadFile(master_pb2.UploadFileRequest(
        filename="test.txt",
        file_size=fileSize
    ))

    assert response.success == True, "UploadFile should succeed"
    assert len(response.assignments) == expectedChunks, \
        f"Expected {expectedChunks} chunks, got {len(response.assignments)}"

    for i, assignment in enumerate(response.assignments):
        assert assignment.chunk_index == i, f"Chunk index mismatch at {i}"
        assert len(assignment.server_addresses) == REPLICATION_FACTOR, \
            f"Expected {REPLICATION_FACTOR} server addresses, got {len(assignment.server_addresses)}"
        assert len(assignment.chunk_handle) > 0, "Chunk handle should not be empty"

    print(f"PASS: UploadFile - {expectedChunks} chunks assigned with {REPLICATION_FACTOR} replicas each")

def testUploadDuplicate(masterStub):
    try:
        masterStub.UploadFile(master_pb2.UploadFileRequest(
            filename="test.txt",
            file_size=1024
        ))
        assert False, "Should have raised an error for duplicate file"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ALREADY_EXISTS, \
            f"Expected ALREADY_EXISTS, got {e.code()}"
    print("PASS: UploadFile duplicate rejected")

def testListFiles(masterStub, expectedCount, testName):
    response = masterStub.ListFiles(master_pb2.ListFilesRequest())
    assert len(response.files) == expectedCount, \
        f"Expected {expectedCount} files, got {len(response.files)}"

    if expectedCount > 0:
        assert response.files[0].filename == "test.txt", \
            f"Expected filename 'test.txt', got '{response.files[0].filename}'"

    print(f"PASS: ListFiles ({testName}) - {expectedCount} files")

def testDownloadFile(masterStub):
    fileSize = 10 * 1024 * 1024
    expectedChunks = math.ceil(fileSize / CHUNK_SIZE)

    response = masterStub.DownloadFile(master_pb2.DownloadFileRequest(
        filename="test.txt"
    ))

    assert response.success == True, "DownloadFile should succeed"
    assert len(response.locations) == expectedChunks, \
        f"Expected {expectedChunks} locations, got {len(response.locations)}"

    for i, location in enumerate(response.locations):
        assert location.chunk_index == i, f"Chunk index mismatch at {i}"
        assert len(location.server_addresses) > 0, "Should have at least one alive server"

    print(f"PASS: DownloadFile - {expectedChunks} chunk locations returned")

def testDownloadNotFound(masterStub):
    try:
        masterStub.DownloadFile(master_pb2.DownloadFileRequest(
            filename="nonexistent.txt"
        ))
        assert False, "Should have raised an error for missing file"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND, \
            f"Expected NOT_FOUND, got {e.code()}"
    print("PASS: DownloadFile not found rejected")

def testDeleteFile(masterStub):
    response = masterStub.DeleteFile(master_pb2.DeleteFileRequest(
        filename="test.txt"
    ))
    assert response.success == True, "DeleteFile should succeed"
    print("PASS: DeleteFile - file marked for deletion")

def testDownloadAfterDelete(masterStub):
    try:
        masterStub.DownloadFile(master_pb2.DownloadFileRequest(
            filename="test.txt"
        ))
        assert False, "Should have raised an error for deleted file"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND, \
            f"Expected NOT_FOUND, got {e.code()}"
    print("PASS: DownloadFile after delete rejected")

def testListDeletedFiles(masterStub):
    response = masterStub.ListDeletedFiles(master_pb2.ListDeletedFilesRequest())
    assert len(response.files) == 1, f"Expected 1 deleted file, got {len(response.files)}"
    entry = response.files[0]
    assert entry.display_name == "test.txt", f"Expected display_name 'test.txt', got '{entry.display_name}'"
    assert entry.internal_name.startswith("._deleted_"), \
        f"Expected internal_name to start with ._deleted_, got '{entry.internal_name}'"
    assert entry.deleted_at_unix > 0, "deleted_at_unix should be set"
    assert entry.seconds_until_gc >= 0, "seconds_until_gc should be non-negative"
    print(f"PASS: ListDeletedFiles - {entry.display_name} ({entry.internal_name})")
    return entry.internal_name

def testRecoverFile(masterStub, internalName):
    response = masterStub.RecoverFile(master_pb2.RecoverFileRequest(filename=internalName))
    assert response.success == True, f"RecoverFile should succeed, got {response.message}"
    assert response.restored_filename == "test.txt", \
        f"Expected restored_filename 'test.txt', got '{response.restored_filename}'"
    print("PASS: RecoverFile - file restored")

def testListAfterRecover(masterStub):
    response = masterStub.ListFiles(master_pb2.ListFilesRequest())
    assert len(response.files) == 1, f"Expected 1 file after recover, got {len(response.files)}"
    assert response.files[0].filename == "test.txt"
    deletedResponse = masterStub.ListDeletedFiles(master_pb2.ListDeletedFilesRequest())
    assert len(deletedResponse.files) == 0, \
        f"Expected 0 deleted files after recover, got {len(deletedResponse.files)}"
    print("PASS: ListFiles after recover - 1 file, 0 deleted")

def testRecoverConflict(masterStub):
    # Re-delete to set up a deleted entry
    masterStub.DeleteFile(master_pb2.DeleteFileRequest(filename="test.txt"))
    # Upload a new live file with the same original name
    masterStub.UploadFile(master_pb2.UploadFileRequest(filename="test.txt", file_size=1024))
    # Find the deleted internal name
    deletedResponse = masterStub.ListDeletedFiles(master_pb2.ListDeletedFilesRequest())
    assert len(deletedResponse.files) == 1
    internalName = deletedResponse.files[0].internal_name
    try:
        masterStub.RecoverFile(master_pb2.RecoverFileRequest(filename=internalName))
        assert False, "Should have raised ALREADY_EXISTS"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.ALREADY_EXISTS, \
            f"Expected ALREADY_EXISTS, got {e.code()}"
    print("PASS: RecoverFile name-conflict rejected")

def testRecoverNotFound(masterStub):
    try:
        masterStub.RecoverFile(master_pb2.RecoverFileRequest(filename="._deleted_0_nope.txt"))
        assert False, "Should have raised NOT_FOUND"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND, \
            f"Expected NOT_FOUND, got {e.code()}"
    print("PASS: RecoverFile not-found rejected")

def main():
    server = startTestServer()
    time.sleep(0.5)  # let server start

    channel = grpc.insecure_channel(f"localhost:{TEST_PORT}")
    masterStub = master_pb2_grpc.MasterServiceStub(channel)
    heartbeatStub = heartbeat_pb2_grpc.HeartbeatServiceStub(channel)

    print("\n--- Master Server Tests ---\n")

    try:
        registerChunkServers(heartbeatStub)
        testUploadFile(masterStub)
        testUploadDuplicate(masterStub)
        testListFiles(masterStub, 1, "after upload")
        testDownloadFile(masterStub)
        testDownloadNotFound(masterStub)
        testDeleteFile(masterStub)
        testListFiles(masterStub, 0, "after delete")
        testDownloadAfterDelete(masterStub)
        internalName = testListDeletedFiles(masterStub)
        testRecoverFile(masterStub, internalName)
        testListAfterRecover(masterStub)
        testRecoverConflict(masterStub)
        testRecoverNotFound(masterStub)

        print("\n--- All tests passed ---\n")
    except AssertionError as e:
        print(f"\nTEST FAILED: {e}\n")
    except Exception as e:
        print(f"\nERROR: {e}\n")
    finally:
        channel.close()
        server.stop(grace=0)

if __name__ == "__main__":
    main()
