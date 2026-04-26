from chunkserver.ngram import NGramIndex


def testIndexChunkBasic():
    ng = NGramIndex()
    ng.indexChunk("c1", "abcabc")
    assert "c1" in ng.index
    assert set(ng.index["c1"].keys()) == {"abc", "bca", "cab"}
    print("PASS: indexChunk produces expected trigrams")


def testCaseInsensitive():
    ng = NGramIndex()
    ng.indexChunk("c1", "ABCabc")
    assert "abc" in ng.index["c1"]
    assert "ABC" not in ng.index["c1"]
    print("PASS: trigrams are lowercased")


def testTokenizeChunkIsPure():
    ng = NGramIndex()
    trigrams = ng.tokenizeChunk("c1", "hello world")
    assert "c1" not in ng.index
    assert "hel" in trigrams
    print("PASS: tokenizeChunk does not mutate shared state")


def testTokenizeMergeEquivalentToIndexChunk():
    a = NGramIndex()
    b = NGramIndex()
    text = "the quick brown fox jumps over the lazy dog"
    a.indexChunk("c1", text)
    trigrams = b.tokenizeChunk("c1", text)
    b.mergeChunk("c1", trigrams)
    assert a.index["c1"] == b.index["c1"]
    print("PASS: tokenize+merge equivalent to indexChunk")


def testCandidatePositionsExactMatch():
    ng = NGramIndex()
    ng.indexChunk("c1", "the quick brown fox")
    assert ng.candidatePositions("c1", "quick") == [4]
    print("PASS: candidatePositions finds exact match")


def testCandidatePositionsMultipleOccurrences():
    ng = NGramIndex()
    ng.indexChunk("c1", "abcXYZabcXYZabc")
    assert ng.candidatePositions("c1", "abc") == [0, 6, 12]
    print("PASS: candidatePositions finds all occurrences")


def testCandidatePositionsNoMatchingTrigram():
    ng = NGramIndex()
    ng.indexChunk("c1", "the quick brown fox")
    assert ng.candidatePositions("c1", "zzz") == []
    print("PASS: returns [] when a query trigram is absent")


def testCandidatePositionsRulesOutFalsePositive():
    ng = NGramIndex()
    ng.indexChunk("c1", "foo123bar")
    assert ng.candidatePositions("c1", "foobar") == []
    print("PASS: multi-trigram intersection filters false positives")


def testCandidatePositionsShortQuery():
    ng = NGramIndex()
    ng.indexChunk("c1", "abcdef")
    assert ng.candidatePositions("c1", "ab") is None
    assert ng.candidatePositions("c1", "a") is None
    assert ng.candidatePositions("c1", "") is None
    print("PASS: queries shorter than 3 chars return None")


def testCandidatePositionsUnindexedChunk():
    ng = NGramIndex()
    assert ng.candidatePositions("nonexistent", "hello") is None
    print("PASS: unindexed chunk returns None")


def testRemoveChunk():
    ng = NGramIndex()
    ng.indexChunk("c1", "abcdef")
    ng.indexChunk("c2", "abcxyz")
    ng.removeChunk("c1")
    assert "c1" not in ng.index
    assert "c2" in ng.index
    ng.removeChunk("c1")  # idempotent
    print("PASS: removeChunk is idempotent")


def testUnicodeCharBasedNotByteBased():
    ng = NGramIndex()
    ng.indexChunk("c1", "café")
    assert "caf" in ng.index["c1"]
    assert "afé" in ng.index["c1"]
    assert ng.candidatePositions("c1", "café") == [0]
    print("PASS: char trigrams handle non-ASCII correctly")


if __name__ == "__main__":
    testIndexChunkBasic()
    testCaseInsensitive()
    testTokenizeChunkIsPure()
    testTokenizeMergeEquivalentToIndexChunk()
    testCandidatePositionsExactMatch()
    testCandidatePositionsMultipleOccurrences()
    testCandidatePositionsNoMatchingTrigram()
    testCandidatePositionsRulesOutFalsePositive()
    testCandidatePositionsShortQuery()
    testCandidatePositionsUnindexedChunk()
    testRemoveChunk()
    testUnicodeCharBasedNotByteBased()
