import os
import shutil
import tempfile
from chunkserver.store import ChunkStore

TEST_DIR = os.path.join(tempfile.gettempdir(), "dfs_test_store")

def setup():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    return ChunkStore(TEST_DIR)

def testWriteAndRead():
    store = setup()
    data = b"hello world this is test data"
    store.writeChunk("chunk-001", data)

    assert store.chunkExists("chunk-001"), "Chunk should exist after write"

    readData = store.readChunk("chunk-001")
    assert readData == data, f"Data mismatch: expected {data}, got {readData}"

    print("PASS: writeChunk and readChunk")

def testReadNonExistent():
    store = setup()
    try:
        store.readChunk("nonexistent")
        assert False, "Should have raised FileNotFoundError"
    except FileNotFoundError:
        pass
    print("PASS: readChunk raises FileNotFoundError for missing chunk")

def testDeleteChunk():
    store = setup()
    store.writeChunk("chunk-002", b"some data")
    assert store.chunkExists("chunk-002"), "Chunk should exist before delete"

    store.deleteChunk("chunk-002")
    assert not store.chunkExists("chunk-002"), "Chunk should not exist after delete"

    print("PASS: deleteChunk")

def testDeleteNonExistent():
    store = setup()
    store.deleteChunk("nonexistent")  # should not crash
    print("PASS: deleteChunk on non-existent chunk does not crash")

def testListChunks():
    store = setup()
    store.writeChunk("chunk-a", b"data a")
    store.writeChunk("chunk-b", b"data b")
    store.writeChunk("chunk-c", b"data c")

    chunks = store.listChunks()
    assert set(chunks) == {"chunk-a", "chunk-b", "chunk-c"}, \
        f"Expected 3 chunks, got {chunks}"

    print("PASS: listChunks")

def testGetAvailableDisk():
    store = setup()
    disk = store.getAvailableDisk()
    assert disk > 0, f"Available disk should be > 0, got {disk}"
    print(f"PASS: getAvailableDisk ({disk / (1024**3):.1f} GB free)")

def testLargeChunk():
    store = setup()
    data = b"x" * (4 * 1024 * 1024)  # 4 MB
    store.writeChunk("chunk-large", data)

    readData = store.readChunk("chunk-large")
    assert len(readData) == len(data), f"Size mismatch: expected {len(data)}, got {len(readData)}"
    assert readData == data, "Large chunk data mismatch"

    print("PASS: 4 MB chunk write and read")

def main():
    print("\n--- ChunkStore Tests ---\n")
    try:
        testWriteAndRead()
        testReadNonExistent()
        testDeleteChunk()
        testDeleteNonExistent()
        testListChunks()
        testGetAvailableDisk()
        testLargeChunk()
        print("\n--- All ChunkStore tests passed ---\n")
    except AssertionError as e:
        print(f"\nTEST FAILED: {e}\n")
    except Exception as e:
        print(f"\nERROR: {e}\n")
    finally:
        if os.path.exists(TEST_DIR):
            shutil.rmtree(TEST_DIR)

if __name__ == "__main__":
    main()
