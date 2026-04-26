from collections import defaultdict
import array
import bisect
import math
import re

from common.config import STOP_WORDS
from chunkserver.varint import encodePositions, decodePositions


class Posting:
    __slots__ = ("chunkHandle", "frequency", "positions")

    chunkHandle: str
    frequency: int
    positions: bytes

    def __init__(self, chunkHandle: str, frequency: int, positions: bytes):
        self.chunkHandle = chunkHandle
        self.frequency = frequency
        self.positions = positions


class InvertedIndex:
    index: dict[str, list[Posting]]
    chunkTerms: dict[str, set[str]]
    lineBreaks: dict[str, "array.array"]

    def __init__(self):
        self.index = defaultdict(list)
        self.chunkTerms = defaultdict(set)
        self.lineBreaks = {}
        self.totalChunks = 0
        self._INDEX_PATTERN = re.compile(r"\w+|\n")
        self._WORD_PATTERN = re.compile(r"\w+")

    def tokenize(self, text: str):
        return [
            match.group()
            for match in self._WORD_PATTERN.finditer(text.lower())
            if match.group() not in STOP_WORDS
        ]

    def tokenizeChunk(self, chunkHandle: str, text: str):
        """
        Pure function - produces postings + linebreaks without touching shared state. 
        Safe to call without a lock.
        """
        termFrequency: dict[str, int] = defaultdict(int)
        termPositions: dict[str, list[int]] = {}
        lineBreaks = array.array("I")

        tokenIndex = 0
        for match in self._INDEX_PATTERN.finditer(text.lower()):
            token = match.group()
            if token == "\n":
                lineBreaks.append(tokenIndex)
                continue
            if token not in STOP_WORDS:
                termFrequency[token] += 1
                positions = termPositions.get(token)
                if positions is None:
                    positions = []
                    termPositions[token] = positions
                positions.append(tokenIndex)
            tokenIndex += 1

        postings = {
            term : Posting(chunkHandle, frequency, encodePositions(termPositions[term]))
            for term, frequency in termFrequency.items()
        }
        return postings, lineBreaks

    def mergeChunk(self, chunkHandle: str, postings: dict, lineBreaks):
        """Mutates shared state. caller must hold write lock"""
        for term, posting in postings.items():
            self.index[term].append(posting)
        self.chunkTerms[chunkHandle] = set(postings.keys())
        self.lineBreaks[chunkHandle] = lineBreaks
        self.totalChunks += 1

    def indexChunk(self, chunkHandle: str, text: str):
        """Convenience wrapper used by sync paths"""
        postings, lineBreaks = self.tokenizeChunk(chunkHandle, text)
        self.mergeChunk(chunkHandle, postings, lineBreaks)

    def removeChunk(self, chunkHandle: str):
        if chunkHandle not in self.chunkTerms:
            return
        for term in self.chunkTerms.pop(chunkHandle):
            self.index[term] = [p for p in self.index[term] if p.chunkHandle != chunkHandle]
            if not self.index[term]:
                del self.index[term]
        self.lineBreaks.pop(chunkHandle, None)
        self.totalChunks -= 1

    def search(self, query: str):
        """Returns [(chunkHandle, score, lineNumber), ...] sorted by score desc.
        Line text is NOT returned — caller looks it up from the chunk store."""
        tokens = self.tokenize(query)
        scores: dict[str, list] = {}
        for token in tokens:
            postings = self.index.get(token, [])
            if not postings:
                continue
            df = len(postings)
            idf = math.log(1 + self.totalChunks / df)
            for posting in postings:
                score = posting.frequency * idf
                firstPosition = next(decodePositions(posting.positions), 0)
                entry = scores.get(posting.chunkHandle)
                if entry is None:
                    scores[posting.chunkHandle] = [score, firstPosition]
                else:
                    entry[0] += score
                    if firstPosition < entry[1]:
                        entry[1] = firstPosition

        ranked = []
        for chunkHandle, (score, firstPosition) in scores.items():
            breaks = self.lineBreaks.get(chunkHandle)
            lineNumber = bisect.bisect_right(breaks, firstPosition) if breaks is not None else 0
            ranked.append((chunkHandle, score, lineNumber))
        ranked.sort(key=lambda x: x[1], reverse=True)
        return ranked
