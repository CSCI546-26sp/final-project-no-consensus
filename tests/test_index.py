from chunkserver.index import InvertedIndex, Posting

def testTokenize():
    idx = InvertedIndex()

    tokens = idx.tokenize("The quick Brown FOX")
    assert "quick" in tokens, "'quick' should be in tokens"
    assert "brown" in tokens, "'brown' should be lowercase"
    assert "fox" in tokens, "'fox' should be lowercase"
    assert "the" not in tokens, "'the' is a stop word"

    print("PASS: tokenize - lowercasing and stop word removal")

def testTokenizeEmpty():
    idx = InvertedIndex()
    tokens = idx.tokenize("")
    assert tokens == [], f"Expected empty list, got {tokens}"

    tokens = idx.tokenize("the a an is are")
    assert tokens == [], f"Expected empty list (all stop words), got {tokens}"

    print("PASS: tokenize - empty and all-stop-words")

def testTokenizeSpecialChars():
    idx = InvertedIndex()
    tokens = idx.tokenize("hello-world, foo@bar! baz")
    assert tokens == ["hello", "world", "foo", "bar", "baz"], \
        f"Expected split on special chars, got {tokens}"

    print("PASS: tokenize - special characters")

def testIndexChunk():
    idx = InvertedIndex()
    text = "the brown fox jumped\nthe brown dog sat\nfox chased the dog"
    idx.indexChunk("chunk-001", text)

    assert idx.totalChunks == 1, f"Expected 1 chunk, got {idx.totalChunks}"
    assert "brown" in idx.index, "'brown' should be indexed"
    assert "fox" in idx.index, "'fox' should be indexed"
    assert "the" not in idx.index, "'the' is a stop word, should not be indexed"

    brownPostings = idx.index["brown"]
    assert len(brownPostings) == 2, f"'brown' appears on 2 lines, got {len(brownPostings)}"
    assert brownPostings[0].frequency == 2, f"'brown' total frequency should be 2, got {brownPostings[0].frequency}"

    foxPostings = idx.index["fox"]
    assert len(foxPostings) == 2, f"'fox' appears on 2 lines, got {len(foxPostings)}"

    print("PASS: indexChunk - basic indexing")

def testIndexChunkPositions():
    idx = InvertedIndex()
    text = "quick brown fox"
    idx.indexChunk("chunk-002", text)

    brownPostings = idx.index["brown"]
    assert brownPostings[0].positions == [1], \
        f"'brown' should be at position 1, got {brownPostings[0].positions}"

    foxPostings = idx.index["fox"]
    assert foxPostings[0].positions == [2], \
        f"'fox' should be at position 2 (after stop word removal), got {foxPostings[0].positions}"

    print("PASS: indexChunk - positions tracked")

def testRemoveChunk():
    idx = InvertedIndex()
    idx.indexChunk("chunk-a", "brown fox jumped")
    idx.indexChunk("chunk-b", "brown dog sat")

    assert idx.totalChunks == 2
    assert len(idx.index["brown"]) == 2, "'brown' should have 2 postings"

    idx.removeChunk("chunk-a")
    assert idx.totalChunks == 1
    assert len(idx.index["brown"]) == 1, "'brown' should have 1 posting after removal"
    assert idx.index["brown"][0].chunkHandle == "chunk-b"
    assert "fox" not in idx.index, "'fox' should be removed entirely"
    assert "jumped" not in idx.index, "'jumped' should be removed entirely"

    print("PASS: removeChunk")

def testSearchSingleTerm():
    idx = InvertedIndex()
    idx.indexChunk("chunk-a", "quantum physics experiment")
    idx.indexChunk("chunk-b", "classical physics theory")
    idx.indexChunk("chunk-c", "biology evolution darwin")

    results = idx.search("physics")
    assert len(results) == 2, f"Expected 2 results, got {len(results)}"

    chunkHandles = [r[0] for r in results]
    assert "chunk-a" in chunkHandles, "chunk-a should match"
    assert "chunk-b" in chunkHandles, "chunk-b should match"
    assert "chunk-c" not in chunkHandles, "chunk-c should not match"

    print("PASS: search - single term")

def testSearchMultiTerm():
    idx = InvertedIndex()
    idx.indexChunk("chunk-a", "quantum physics experiment")
    idx.indexChunk("chunk-b", "quantum biology fusion")
    idx.indexChunk("chunk-c", "classical physics theory")

    results = idx.search("quantum physics")
    assert len(results) == 3, f"Expected 3 results, got {len(results)}"

    # chunk-a has both terms, should score highest
    assert results[0][0] == "chunk-a", \
        f"chunk-a should rank first (has both terms), got {results[0][0]}"

    print("PASS: search - multi term ranking")

def testSearchNoResults():
    idx = InvertedIndex()
    idx.indexChunk("chunk-a", "hello world")

    results = idx.search("nonexistent")
    assert len(results) == 0, f"Expected 0 results, got {len(results)}"

    print("PASS: search - no results")

def testSearchStopWordsOnly():
    idx = InvertedIndex()
    idx.indexChunk("chunk-a", "hello world")

    results = idx.search("the and or")
    assert len(results) == 0, f"Expected 0 results (all stop words), got {len(results)}"

    print("PASS: search - stop words only query")

def main():
    print("\n--- InvertedIndex Tests ---\n")
    try:
        testTokenize()
        testTokenizeEmpty()
        testTokenizeSpecialChars()
        testIndexChunk()
        testIndexChunkPositions()
        testRemoveChunk()
        testSearchSingleTerm()
        testSearchMultiTerm()
        testSearchNoResults()
        testSearchStopWordsOnly()
        print("\n--- All InvertedIndex tests passed ---\n")
    except AssertionError as e:
        print(f"\nTEST FAILED: {e}\n")
    except Exception as e:
        print(f"\nERROR: {e}\n")

if __name__ == "__main__":
    main()
