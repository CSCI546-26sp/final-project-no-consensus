from chunkserver.index import InvertedIndex, Posting, _decodePositions


def testTokenize():
    idx = InvertedIndex()
    tokens = idx.tokenize("The quick Brown FOX")
    assert "quick" in tokens
    assert "brown" in tokens
    assert "fox" in tokens
    assert "the" not in tokens
    print("PASS: tokenize - lowercasing and stop word removal")


def testTokenizeEmpty():
    idx = InvertedIndex()
    assert idx.tokenize("") == []
    assert idx.tokenize("the a an is are") == []
    print("PASS: tokenize - empty and all-stop-words")


def testTokenizeSpecialChars():
    idx = InvertedIndex()
    tokens = idx.tokenize("hello-world, foo@bar! baz")
    assert tokens == ["hello", "world", "foo", "bar", "baz"], \
        f"Expected split on special chars, got {tokens}"
    print("PASS: tokenize - special characters")


def testIndexChunkOnePostingPerChunk():
    idx = InvertedIndex()
    text = "the brown fox jumped\nthe brown dog sat\nfox chased the dog"
    idx.indexChunk("chunk-001", text)

    assert idx.totalChunks == 1
    assert "brown" in idx.index
    assert "fox" in idx.index
    assert "the" not in idx.index, "stop word should not be indexed"

    brownPostings = idx.index["brown"]
    assert len(brownPostings) == 1, \
        f"expected 1 posting per (term, chunk), got {len(brownPostings)}"
    assert brownPostings[0].chunkHandle == "chunk-001"
    assert brownPostings[0].frequency == 2, \
        f"'brown' frequency should be 2, got {brownPostings[0].frequency}"

    foxPostings = idx.index["fox"]
    assert len(foxPostings) == 1
    assert foxPostings[0].frequency == 2, \
        f"'fox' frequency should be 2, got {foxPostings[0].frequency}"

    print("PASS: indexChunk - one posting per (term, chunk)")


def testIndexChunkPositionsStopWordsOccupySlots():
    idx = InvertedIndex()
    idx.indexChunk("chunk-002", "the brown fox")

    brownPositions = list(_decodePositions(idx.index["brown"][0].positions))
    foxPositions = list(_decodePositions(idx.index["fox"][0].positions))
    assert brownPositions == [1], \
        f"'brown' should be at position 1 (after 'the' slot), got {brownPositions}"
    assert foxPositions == [2], \
        f"'fox' should be at position 2, got {foxPositions}"
    assert "the" not in idx.index
    print("PASS: indexChunk - stop words occupy position slots")


def testIndexChunkBytesType():
    idx = InvertedIndex()
    idx.indexChunk("c", "quick brown fox")
    positions = idx.index["brown"][0].positions
    assert isinstance(positions, bytes), \
        f"positions should be bytes (varint-encoded), got {type(positions)}"
    print("PASS: indexChunk - positions stored as varint bytes")


def testLineBreaksTracked():
    idx = InvertedIndex()
    idx.indexChunk("c", "alpha beta\ngamma delta\nepsilon")

    # tokens: alpha(0) beta(1) [\n@2] gamma(2) delta(3) [\n@4] epsilon(4)
    lineBreaks = list(idx.lineBreaks["c"])
    assert lineBreaks == [2, 4], \
        f"expected lineBreaks=[2,4], got {lineBreaks}"
    print("PASS: lineBreaks - tracked at correct token indexes")


def testLineNumberDerivation():
    idx = InvertedIndex()
    idx.indexChunk("c", "alpha beta\ngamma delta\nepsilon")

    assert idx.search("alpha")[0][2] == 0, "alpha is on line 0"
    assert idx.search("gamma")[0][2] == 1, "gamma is on line 1"
    assert idx.search("epsilon")[0][2] == 2, "epsilon is on line 2"
    print("PASS: search - line number derived via bisect over lineBreaks")


def testRemoveChunk():
    idx = InvertedIndex()
    idx.indexChunk("chunk-a", "brown fox jumped")
    idx.indexChunk("chunk-b", "brown dog sat")

    assert idx.totalChunks == 2
    assert len(idx.index["brown"]) == 2

    idx.removeChunk("chunk-a")

    assert idx.totalChunks == 1
    assert len(idx.index["brown"]) == 1
    assert idx.index["brown"][0].chunkHandle == "chunk-b"
    assert "fox" not in idx.index, "'fox' should be removed entirely"
    assert "jumped" not in idx.index
    assert "chunk-a" not in idx.chunkTerms, "chunkTerms should be cleared"
    assert "chunk-a" not in idx.lineBreaks, "lineBreaks should be cleared"

    print("PASS: removeChunk - index, chunkTerms, lineBreaks cleaned")


def testSearchSingleTerm():
    idx = InvertedIndex()
    idx.indexChunk("chunk-a", "quantum physics experiment")
    idx.indexChunk("chunk-b", "classical physics theory")
    idx.indexChunk("chunk-c", "biology evolution darwin")

    results = idx.search("physics")
    assert len(results) == 2, f"Expected 2 results, got {len(results)}"

    handles = [r[0] for r in results]
    assert "chunk-a" in handles
    assert "chunk-b" in handles
    assert "chunk-c" not in handles
    print("PASS: search - single term")


def testSearchMultiTerm():
    idx = InvertedIndex()
    idx.indexChunk("chunk-a", "quantum physics experiment")
    idx.indexChunk("chunk-b", "quantum biology fusion")
    idx.indexChunk("chunk-c", "classical physics theory")

    results = idx.search("quantum physics")
    assert len(results) == 3
    assert results[0][0] == "chunk-a", \
        f"chunk-a should rank first (has both terms), got {results[0][0]}"
    print("PASS: search - multi-term ranking")


def testSearchNoResults():
    idx = InvertedIndex()
    idx.indexChunk("chunk-a", "hello world")
    assert idx.search("nonexistent") == []
    print("PASS: search - no results")


def testSearchStopWordsOnly():
    idx = InvertedIndex()
    idx.indexChunk("chunk-a", "hello world")
    assert idx.search("the and or") == []
    print("PASS: search - stop words only query")


def testTfInflationRegression():
    """Task #12 guard: term on many lines must not get per-line score inflation."""
    idx = InvertedIndex()
    idx.indexChunk("chunk-a", "cat sat on mat")
    idx.indexChunk("chunk-b", "cat\ncat\ncat\ncat\ncat")

    results = idx.search("cat")
    scoreA = next(s for h, s, _ in results if h == "chunk-a")
    scoreB = next(s for h, s, _ in results if h == "chunk-b")
    ratio = scoreB / scoreA if scoreA else float("inf")
    assert 4.5 < ratio < 5.5, \
        f"chunk-b should score ~5x chunk-a (tf ratio), got ratio={ratio:.2f}"
    print("PASS: search - TF inflation fixed (task #12)")


def testAdjacentPositionsForPhraseSearch():
    """Scaffolding for task #14: adjacent terms in text must have adjacent
    integer positions (stop words occupy slots so phrase search intersects cleanly)."""
    idx = InvertedIndex()
    idx.indexChunk("c", "the quick brown fox the lazy dog")

    quickPos = list(_decodePositions(idx.index["quick"][0].positions))
    brownPos = list(_decodePositions(idx.index["brown"][0].positions))
    foxPos = list(_decodePositions(idx.index["fox"][0].positions))
    lazyPos = list(_decodePositions(idx.index["lazy"][0].positions))
    dogPos = list(_decodePositions(idx.index["dog"][0].positions))

    assert quickPos == [1]
    assert brownPos == [2]
    assert foxPos == [3]
    # 'the' at slot 4 is a stop word — occupies the slot but isn't indexed
    assert lazyPos == [5]
    assert dogPos == [6]
    print("PASS: adjacent terms have adjacent positions (phrase-search ready)")


def main():
    print("\n--- InvertedIndex Tests ---\n")
    try:
        testTokenize()
        testTokenizeEmpty()
        testTokenizeSpecialChars()
        testIndexChunkOnePostingPerChunk()
        testIndexChunkPositionsStopWordsOccupySlots()
        testIndexChunkBytesType()
        testLineBreaksTracked()
        testLineNumberDerivation()
        testRemoveChunk()
        testSearchSingleTerm()
        testSearchMultiTerm()
        testSearchNoResults()
        testSearchStopWordsOnly()
        testTfInflationRegression()
        testAdjacentPositionsForPhraseSearch()
        print("\n--- All InvertedIndex tests passed ---\n")
    except AssertionError as e:
        print(f"\nTEST FAILED: {e}\n")
    except Exception as e:
        print(f"\nERROR: {e}\n")


if __name__ == "__main__":
    main()
