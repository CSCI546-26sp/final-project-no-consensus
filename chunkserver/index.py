from collections import defaultdict
from common.config import STOP_WORDS
import re
import math

class Posting:
    chunkHandle: str
    lineNumber: int
    frequency: int
    lineText: str

    #This is for phrase matching. Not implementing 
    #phrase matching in this iteration; will implement
    #it in the future depending on time availability
    positions: list[int]

    def __init__(self, chunkHandle, lineNumber, 
                 frequency, lineText, positions = None):
        
        self.chunkHandle = chunkHandle
        self.lineNumber = lineNumber
        self.frequency = frequency
        self.lineText = lineText
        self.positions = positions or []

class InvertedIndex:
    index: dict[str, list[Posting]]
    chunkTerms: dict[str, set[str]]

    def __init__(self):
        self.index = defaultdict(list)
        self.totalChunks = 0
        self.chunkTerms = defaultdict(set)
        self._TOKEN_PATTERN = re.compile(r'\W+')

    def tokenize(self, text: str):
        return list(filter(
            lambda w: w and w not in STOP_WORDS, 
            self._TOKEN_PATTERN.split(text.lower())))
    
    def indexChunk(self, chunkHandle: str, text: str):
        lines = text.split('\n')
        chunkTermfrequency = defaultdict(int)
        termLines = defaultdict(list)

        for lineNumber, line in enumerate(lines):
            tokens = self.tokenize(line)
            linePositions = defaultdict(list)
            
            for position, token in enumerate(tokens):
                chunkTermfrequency[token] += 1
                linePositions[token].append(position)

            for token, positions in linePositions.items():
                termLines[token].append((lineNumber, line, positions))

        for term, locations in termLines.items():
            for lineNumber, lineText, positions in locations:
                self.index[term].append(
                    Posting(chunkHandle, lineNumber, 
                            chunkTermfrequency[term], lineText, positions)
                )
        self.chunkTerms[chunkHandle] = set(termLines.keys())
        self.totalChunks += 1
        return
    
    def removeChunk(self, chunkHandle):
        # for term in list(self.index.keys()):
        for term in self.chunkTerms.pop(chunkHandle, set()):
            self.index[term] = [p for p in self.index[term] if p.chunkHandle != chunkHandle]
            if not self.index[term]:
                del self.index[term]
        self.totalChunks -= 1
        return 
        
    def search(self, query):
        tokens = self.tokenize(query)
        result: dict[str, list[int | str]] = {}
        for token in tokens:
            postings = self.index.get(token, [])
            if not postings:
                continue
            idf = math.log(self.totalChunks / len(postings))
            for posting in postings:
                tf = posting.frequency
                score = tf * idf

                if posting.chunkHandle not in result:
                    result[posting.chunkHandle] = [0, posting.lineNumber, posting.lineText]
                result[posting.chunkHandle][0] += score

        return sorted(result.items(), key=lambda x: x[1][0], reverse=True)
    

                    

